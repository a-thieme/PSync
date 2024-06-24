/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2014-2023,  The University of Memphis
 *
 * This file is part of PSync.
 * See AUTHORS.md for complete list of PSync authors and contributors.
 *
 * PSync is free software: you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * PSync is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with
 * PSync, e.g., in COPYING.md file.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "PSync/consumer.hpp"
#include "PSync/detail/state.hpp"

#include <ndn-cxx/lp/tags.hpp>
#include <ndn-cxx/security/validator-null.hpp>
#include <ndn-cxx/util/logger.hpp>

namespace psync {

NDN_LOG_INIT(psync.Consumer);

Consumer::Consumer(ndn::Face& face, const ndn::Name& syncPrefix, const Options& opts)
  : m_face(face)
  , m_scheduler(m_face.getIoContext())
  , m_syncPrefix(syncPrefix)
  , m_defaultInterestPrefix(ndn::Name(m_syncPrefix).append(DEFAULT))
  , m_syncInterestPrefix(ndn::Name(m_syncPrefix).append(SYNC))
  , m_syncDataContentType(ndn::tlv::ContentType_Blob)
  , m_onReceiveDefaultData(opts.onDefaultData)
  , m_onUpdate(opts.onUpdate)
  , m_bloomFilter(opts.bfCount, opts.bfFalsePositive)
  , m_defaultInterestLifetime(opts.defaultInterestLifetime)
  , m_syncInterestLifetime(opts.syncInterestLifetime)
  , m_rng(ndn::random::getRandomNumberEngine())
  , m_rangeUniformRandom(100, 500)
{
  addSubscription(m_syncPrefix.append(DEFAULT), 0);
}

Consumer::Consumer(const ndn::Name& syncPrefix,
                   ndn::Face& face,
                   const ReceiveDefaultCallback& onReceiveDefaultData,
                   const UpdateCallback& onUpdate,
                   unsigned int count,
                   double falsePositive,
                   ndn::time::milliseconds defaultInterestLifetime,
                   ndn::time::milliseconds syncInterestLifetime)
  : Consumer(face, syncPrefix,
             Options{onReceiveDefaultData, onUpdate, static_cast<uint32_t>(count), falsePositive,
                     defaultInterestLifetime, syncInterestLifetime})
{
}

bool
Consumer::addSubscription(const ndn::Name& prefix, uint64_t seqNo, bool callSyncDataCb)
{
  NDN_LOG_DEBUG("addSubscription: " << prefix << " " << seqNo);
  auto it = m_prefixes.emplace(prefix, seqNo);
  if (!it.second) {
    return false;
  }

  NDN_LOG_DEBUG("Subscribing prefix: " << prefix);

  m_subscriptionList.emplace(prefix);
  NDN_LOG_DEBUG("subscribed prefix: " << prefix);
  m_bloomFilter.insert(prefix);
  NDN_LOG_DEBUG("inserted prefix: " << prefix);

  // we will only get a sequence number for subscribed streams when we are out of sync
  // since we want a sequence number for a new subscription, we need the producer to
  // think we are out of sync
  m_iblt = EMPTY_IBLT;

  if (callSyncDataCb && seqNo != 0) {
    m_onUpdate({{prefix, seqNo, seqNo, 0}});
  }

  return true;
}

bool
Consumer::removeSubscription(const ndn::Name& prefix)
{
  if (!isSubscribed(prefix))
    return false;

  NDN_LOG_DEBUG("Unsubscribing prefix: " << prefix);

  m_prefixes.erase(prefix);
  m_subscriptionList.erase(prefix);

  // Clear and reconstruct the bloom filter
  m_bloomFilter.clear();

  for (const auto& item : m_subscriptionList)
    m_bloomFilter.insert(item);

  return true;
}

void
Consumer::stop()
{
  NDN_LOG_DEBUG("Canceling all the scheduled events");
  m_scheduler.cancelAllEvents();

  if (m_syncFetcher) {
    m_syncFetcher->stop();
    m_syncFetcher.reset();
  }

  if (m_defaultFetcher) {
    m_defaultFetcher->stop();
    m_defaultFetcher.reset();
  }
}

void
Consumer::sendDefaultInterest()
{
  ndn::Interest defaultInterest(m_defaultInterestPrefix);
  NDN_LOG_DEBUG("Send Default Interest " << defaultInterest);

  if (m_defaultFetcher) {
    m_defaultFetcher->stop();
  }

  using ndn::SegmentFetcher;
  SegmentFetcher::Options options;
  options.interestLifetime = m_defaultInterestLifetime;
  options.maxTimeout = m_defaultInterestLifetime;
  options.rttOptions.initialRto = m_syncInterestLifetime;

  m_defaultFetcher = SegmentFetcher::start(m_face, defaultInterest,
                                         ndn::security::getAcceptAllValidator(), options);

  // this is just for getting the IBF from the "hello" (default) data name
  m_defaultFetcher->afterSegmentValidated.connect([this] (const ndn::Data& data) {
    NDN_LOG_DEBUG("Data for " << data.getFullName() << " is validated.");
  });

  m_defaultFetcher->onComplete.connect([this] (const ndn::ConstBufferPtr& bufferPtr) {
    onDefaultData(bufferPtr);
  });

  m_defaultFetcher->onError.connect([this] (uint32_t errorCode, const std::string& msg) {
    NDN_LOG_TRACE("Cannot fetch default data, error: " << errorCode << " message: " << msg);
    ndn::time::milliseconds after(m_rangeUniformRandom(m_rng));
    NDN_LOG_TRACE("Scheduling after " << after);
    m_scheduler.schedule(after, [this] { sendDefaultInterest(); });
  });
}

void
Consumer::onDefaultData(const ndn::ConstBufferPtr &bufferPtr)
{
  NDN_LOG_TRACE("onDefaultData");

  detail::State state{ndn::Block(bufferPtr)};
  std::vector<ndn::Name> availableSubscriptions;

  NDN_LOG_DEBUG("Default Data: " << state);

  for (const ndn::Name prefix : state) {
    NDN_LOG_TRACE("onDefaultData prefix: " << prefix);
    if (!m_defaultInterestPrefix.equals(prefix))
    {
      NDN_LOG_DEBUG("new stream available: " << prefix);
      availableSubscriptions.emplace_back(prefix);
    }
  }
  if (!availableSubscriptions.empty()) {
    NDN_LOG_DEBUG("do on default data callback because no available subscriptions");
    m_onReceiveDefaultData(availableSubscriptions);
  }
}

void
Consumer::sendSyncInterest()
{
  // /sync/sync/
  ndn::Name syncInterestName(m_syncInterestPrefix);

  // Append subscription list
  // /sync/sync/<SL>/
  m_bloomFilter.appendToName(syncInterestName);

  // /sync/sync/<SL>/<IBF>/
  if (m_sendEmptyIBLT) {
    // doing it this way instead of as function parameter because we would then
    // have multiple function calls to sendSyncInterest, resulting in sending them
    // more often than we want. We have suppression code to help in these cases,
    // if they do happen

    // we're not just changing m_iblt because getting new sync data after m_iblt
    // is set to EMPTY_IBLT but before that interest is sent with EMPTY_IBLT will
    // override the empty value and we won't get all the sequence numbers we need.

    // when m_sendEmptyIBLT is set to true, it should result in only one sync
    // interest being sent with the empty IBLT
    syncInterestName.append(EMPTY_IBLT);
    m_sendEmptyIBLT = false;
  } else {
    // Append IBF received in default/sync data
    syncInterestName.append(m_iblt);
  }

  auto currentTime = ndn::time::system_clock::now();
  if ((currentTime - m_lastInterestSentTime < MIN_JITTER + SYNC_INTEREST_LIFETIME/2) &&
      (m_outstandingInterestName == syncInterestName)) {
    NDN_LOG_TRACE("Suppressing Interest: " << std::hash<ndn::Name>{}(syncInterestName));
    return;
  }

  m_scheduler.schedule(m_syncInterestLifetime / 2 + ndn::time::milliseconds(m_jitter(m_rng)),
                           [this] { sendSyncInterest(); });

  m_outstandingInterestName = syncInterestName;
  ndn::Interest syncInterest(syncInterestName);

  NDN_LOG_TRACE("sendSyncInterest, nonce: " << syncInterest.getNonce() <<
                " hash: " << std::hash<ndn::Name>{}(syncInterest.getName()));
  NDN_LOG_TRACE("sync interest name: " << syncInterestName);

  if (m_syncFetcher) {
    m_syncFetcher->stop();
  }

  using ndn::SegmentFetcher;
  SegmentFetcher::Options options;
  options.interestLifetime = m_syncInterestLifetime;
  options.maxTimeout = m_syncInterestLifetime;
  options.rttOptions.initialRto = m_syncInterestLifetime;

  m_lastInterestSentTime = currentTime;
  m_syncFetcher = SegmentFetcher::start(m_face, syncInterest,
                                        ndn::security::getAcceptAllValidator(), options);

  m_syncFetcher->afterSegmentValidated.connect([this] (const ndn::Data& data) {
    // copied from full-producer.cpp
    auto tag = data.getTag<ndn::lp::IncomingFaceIdTag>();
    if (tag) {
      m_incomingFace = *tag;
    }
    else {
      m_incomingFace = 0;
    }
    if (data.getFinalBlock()) {
      m_syncDataName = data.getName().getPrefix(-2);
      NDN_LOG_TRACE("sync data name: " << m_syncDataName);
      m_syncDataContentType = data.getContentType();
      NDN_LOG_TRACE("sync data content type: " << m_syncDataContentType);
    }

    if (m_syncDataContentType == ndn::tlv::ContentType_Nack) {
      NDN_LOG_WARN("Producer couldn't decode the difference, sending sync interest with empty IBF");
      m_sendEmptyIBLT = true;
    }
  });

  m_syncFetcher->onComplete.connect([this] (const ndn::ConstBufferPtr& bufferPtr) {
    if (m_syncDataContentType == ndn::tlv::ContentType_Nack) {
      NDN_LOG_TRACE("Segment fetcher completed with content type NACK");
      m_syncDataContentType = ndn::tlv::ContentType_Blob;
      return;
    }
    NDN_LOG_TRACE("Segment fetcher got sync data");
    onSyncData(bufferPtr);
  });

  m_syncFetcher->onError.connect([this] (uint32_t errorCode, const std::string& msg) {
    if (errorCode == SegmentFetcher::ErrorCode::INTEREST_TIMEOUT) {
      // see full-producer.cpp for explanation
      NDN_LOG_TRACE("sync interest timed out");
    } else {
      // warn because something else went wrong
      NDN_LOG_WARN("Cannot fetch sync data, error: " << errorCode << " message: " << msg);
      ndn::time::milliseconds after(m_rangeUniformRandom(m_rng));
      NDN_LOG_TRACE("Scheduling sync Interest after: " << after);
      m_scheduler.schedule(after, [this] { sendSyncInterest(); });
    }
  });
}

void
Consumer::onSyncData(const ndn::ConstBufferPtr& bufferPtr)
{
  // Extract IBF from sync data name which is the last component
  m_iblt = m_syncDataName.get(m_syncDataName.size() - 1);

  detail::State state{ndn::Block(bufferPtr)};
  std::vector<MissingDataInfo> updates;

  for (const auto& content : state) {
    NDN_LOG_DEBUG(content);
    const ndn::Name& prefix = content.getPrefix(-1);
    uint64_t seq = content.get(content.size() - 1).toNumber();
    // false positives can occur when the producer checks for updates that match our subscriptions list,
    // so we have to check to make sure the prefix is in our subscription list (m_prefixes)
    if (m_prefixes.find(prefix) == m_prefixes.end()) {
      // possible security concerns may occur if access control is done on the "default" stream because
      // false positives will leak available prefix names
      NDN_LOG_WARN("False positive detected for prefix " << prefix);
    } else if (seq > m_prefixes[prefix]) {
      // consumer subscribed to the prefix and there is an update to that stream
      if (prefix.equals(m_defaultInterestPrefix)) {
        // send an interest for "default" data since there's an update to it
        sendDefaultInterest();
      } else {
        // If this is just the next seq number then we had already informed the consumer about
        // the previous sequence number and hence seq low and seq high should be equal to current seq
        updates.push_back({prefix, m_prefixes[prefix] + 1, seq, 0});
      }
      m_prefixes[prefix] = seq;
    } else {
      // consumer got "update" for subscribed stream, but the "new" sequence number is the same as
      // the consumer's "old" sequence
      NDN_LOG_WARN("Got update for prefix " << prefix << " but the sequence number is not new: " << m_prefixes[prefix] << " <= " << seq);
    }
  }

  NDN_LOG_DEBUG("Sync Data: " << state);

  if (!updates.empty()) {
    m_onUpdate(updates);
  }
}

} // namespace psync
