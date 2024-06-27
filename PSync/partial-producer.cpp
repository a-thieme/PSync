/* -*- Mode:C++; c-file-style:"gnu"; indent-tabs-mode:nil; -*- */
/*
 * Copyright (c) 2014-2024,  The University of Memphis
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
 **/

#include "PSync/partial-producer.hpp"
#include "PSync/detail/state.hpp"

#include <ndn-cxx/util/logger.hpp>

#include <cstring>

namespace psync {

NDN_LOG_INIT(psync.PartialProducer);

PartialProducer::PartialProducer(ndn::Face& face,
                                 ndn::KeyChain& keyChain,
                                 const ndn::Name& syncPrefix,
                                 const Options& opts)
  : ProducerBase(face, keyChain, opts.ibfCount, syncPrefix, opts.syncDataFreshness,
                 opts.ibfCompression, CompressionScheme::NONE)
  , m_defaultReplyFreshness(opts.defaultDataFreshness)
{
  m_registeredPrefix = m_face.registerPrefix(m_syncPrefix,
    [this] (const auto&) {
      m_face.setInterestFilter(ndn::Name(m_syncPrefix).append(SYNC),
                               std::bind(&PartialProducer::onSyncInterest, this, _1, _2));
      m_face.setInterestFilter(ndn::Name(m_syncPrefix).append(DEFAULT),
                               std::bind(&PartialProducer::onDefaultInterest, this, _1, _2));
    },
    [] (auto&&... args) { onRegisterFailed(std::forward<decltype(args)>(args)...); });
    // Add default stream at the start
    addUserNode(ndn::Name(m_syncPrefix).append(DEFAULT));
}

PartialProducer::PartialProducer(ndn::Face& face,
                                 ndn::KeyChain& keyChain,
                                 size_t expectedNumEntries,
                                 const ndn::Name& syncPrefix,
                                 const ndn::Name& userPrefix,
                                 ndn::time::milliseconds defaultReplyFreshness,
                                 ndn::time::milliseconds syncReplyFreshness,
                                 CompressionScheme ibltCompression)
  : PartialProducer(face, keyChain, syncPrefix,
                    Options{static_cast<uint32_t>(expectedNumEntries), ibltCompression,
                            defaultReplyFreshness, syncReplyFreshness})
{
  addUserNode(userPrefix);
}

bool
PartialProducer::addUserNode(const ndn::Name& prefix)
{
  // NDN_LOG_INFO("Add user Node: " << prefix );
  bool result = ProducerBase::addUserNode(prefix);
  if (result) {
    updateDefaultSeqNo();
  }
  return result;
}


void
PartialProducer::publishName(const ndn::Name& prefix, std::optional<uint64_t> seq)
{
  if (m_prefixes.find(prefix) == m_prefixes.end()) {
    return;
  }

  uint64_t newSeq = seq.value_or(m_prefixes[prefix] + 1);
  NDN_LOG_INFO("Publish: " << prefix << "/" << newSeq);
  updateSeqNo(prefix, newSeq);
  satisfyPendingSyncInterests(prefix);
}

void PartialProducer::updateDefaultSeqNo()
{
  const auto prefix = ndn::Name(m_syncPrefix).append(DEFAULT);
  uint64_t newSeq = m_prefixes[prefix] + 1;
  NDN_LOG_INFO("Publish: " << prefix << "/" << newSeq);
  updateSeqNo(prefix, newSeq);
}

void
PartialProducer::onDefaultInterest(const ndn::Name& prefix, const ndn::Interest& interest)
{
  NDN_LOG_WARN("Default Interest Received: " << interest);
  const auto& name = interest.getName();
  auto seq = getSeqNo(name.getPrefix(-1));
  if (!seq.has_value()) {
    NDN_LOG_WARN("Got interest for default stream before it has a sequence number");
    return;
  }
  auto seqFromInterest = name.get(-1).toNumber();
  if (seqFromInterest != seq.value()) {
    NDN_LOG_WARN("Got interest for default stream with sequence number " << seqFromInterest << " that does not match the latest sequence number " << seq.value());
    return;
  }

  if (m_segmentPublisher.replyFromStore(name)) {
    return;
  }

  detail::State state;
  for (const auto& p : m_prefixes) {
    state.addContent(ndn::Name(p.first));
  }
  NDN_LOG_DEBUG("sending content: " << state);

  m_segmentPublisher.publish(interest.getName(), name,
                             state.wireEncode(), m_defaultReplyFreshness);
}


void
PartialProducer::onSyncInterest(const ndn::Name& prefix, const ndn::Interest& interest)
{
  if (m_segmentPublisher.replyFromStore(interest.getName())) {
    return;
  }

  NDN_LOG_DEBUG("Sync Interest Received, nonce: " << interest.getNonce() <<
                " hash: " << std::hash<ndn::Name>{}(interest.getName()));

  ndn::Name nameWithoutSyncPrefix = interest.getName().getSubName(prefix.size());
  ndn::Name interestName;
  NDN_LOG_DEBUG("full interest name " << interest.getName());
  NDN_LOG_DEBUG("sync interest prefix " << prefix);
  NDN_LOG_DEBUG("name without prefix " << nameWithoutSyncPrefix);

  switch (nameWithoutSyncPrefix.size()) {
    case 4:
      NDN_LOG_TRACE("nameWithoutSyncPrefix.size() 4");
      // Get /<prefix>/<SL>/IBF/ from /<prefix>/BF/IBF (3 components of BF + 1 for IBF)
      interestName = interest.getName();
      break;
    case 6:
      NDN_LOG_TRACE("nameWithoutSyncPrefix.size() 6");
      // Get <prefix>/<SL>/IBF/ from /<prefix>/BF/IBF/<version>/<segment-no>
      interestName = interest.getName().getPrefix(-2);
      break;
    default:
      NDN_LOG_WARN("name without sync prefix is of length " << nameWithoutSyncPrefix.size());
      NDN_LOG_WARN("aborting response to sync interest " << interest.getName());
      return;
  }

  // /projected-count/false-positive per 1000/bf name/ibltname
  ndn::name::Component bfName, ibltName;
  unsigned int projectedCount;
  double falsePositiveProb;
  try {
    projectedCount = nameWithoutSyncPrefix.get(0).toNumber();
    NDN_LOG_DEBUG("projected count: " << projectedCount);

    falsePositiveProb = nameWithoutSyncPrefix.get(1).toNumber()/1000.;
    NDN_LOG_DEBUG("false positive: " << falsePositiveProb);

    bfName = nameWithoutSyncPrefix.get(2);
    NDN_LOG_DEBUG("subscription list: " << bfName);

    ibltName = nameWithoutSyncPrefix.get(3);
    NDN_LOG_DEBUG("iblt name: " << ibltName);
  }
  catch (const std::exception& e) {
    NDN_LOG_ERROR("Cannot extract subscription list and IBF from sync interest: " << e.what());
    NDN_LOG_ERROR("Format: /<syncPrefix>/sync/<BF-count>/<BF-false-positive-probability>/<BF>/<IBF>");
    return;
  }

  detail::BloomFilter bf;
  try {
    bf = detail::BloomFilter(projectedCount, falsePositiveProb, bfName);
  }
  catch (const std::exception& e) {
    NDN_LOG_DEBUG("had exception in bf bloom filter initialization");
    NDN_LOG_WARN(e.what());
    return;
  }

  detail::IBLT iblt(m_expectedNumEntries, m_ibltCompression);
  if (ibltName == EMPTY_IBLT) {
    NDN_LOG_TRACE("consumer is requesting new ibf or doesn't know producer parameters");
  } else {
    try {
      iblt.initialize(ibltName);
    } catch (const std::exception& e) {
      NDN_LOG_DEBUG("had exception in iblt bloom filter initialization");
      NDN_LOG_WARN(e.what());
      return;
    }
  }

  // get the difference
  // non-empty positive means we have some elements that the others don't
  auto diff = m_iblt - iblt;

  NDN_LOG_TRACE("Number elements in IBF: " << m_prefixes.size());

  NDN_LOG_TRACE("diff.canDecode: " << diff.canDecode);

  if (!diff.canDecode) {
    NDN_LOG_DEBUG("Can't decode the difference, sending application Nack");
    sendApplicationNack(interestName);
    return;
  }

  // generate content for Sync reply
  detail::State state;
  NDN_LOG_TRACE("Size of positive set " << diff.positive.size());
  NDN_LOG_TRACE("Size of negative set " << diff.negative.size());
  for (const auto& hash : diff.positive) {
    // full data stream name as /name/<seq no>
    auto nameIt = m_biMap.left.find(hash);
    if (nameIt != m_biMap.left.end()) {
      if (bf.contains(nameIt->second.getPrefix(-1))) {
        // generate data
        state.addContent(nameIt->second);
        NDN_LOG_DEBUG("Content: " << nameIt->second << " " << nameIt->first);
      }
    }
  }

  NDN_LOG_TRACE("m_threshold: " << m_threshold << " Total: " << diff.positive.size() + diff.negative.size());

  if (diff.positive.size() + diff.negative.size() >= m_threshold || !state.getContent().empty()) {

    // send back data
    ndn::Name syncDataName = interestName;
    m_iblt.appendToName(syncDataName);

    m_segmentPublisher.publish(interest.getName(), syncDataName,
                               state.wireEncode(), m_syncReplyFreshness);
    return;
  }

  auto& entry = m_pendingEntries.emplace(interestName, PendingEntryInfo{bf, iblt, {}}).first->second;
  entry.expirationEvent = m_scheduler.schedule(interest.getInterestLifetime(),
                          [this, interest] {
                            NDN_LOG_TRACE("Erase Pending Interest " << interest.getNonce());
                            m_pendingEntries.erase(interest.getName());
                          });
}

void
PartialProducer::satisfyPendingSyncInterests(const ndn::Name& prefix) {
  NDN_LOG_TRACE("size of pending interest: " << m_pendingEntries.size());

  for (auto it = m_pendingEntries.begin(); it != m_pendingEntries.end();) {
    const PendingEntryInfo& entry = it->second;

    auto diff = m_iblt - entry.iblt;

    NDN_LOG_TRACE("diff.canDecode: " << diff.canDecode);

    NDN_LOG_TRACE("Number elements in IBF: " << m_prefixes.size());
    NDN_LOG_TRACE("m_threshold: " << m_threshold << " Total: " << diff.positive.size() + diff.negative.size());

    if (!diff.canDecode) {
      NDN_LOG_TRACE("Decoding of differences with stored IBF unsuccessful, deleting pending interest");
      m_pendingEntries.erase(it++);
      continue;
    }

    detail::State state;
    if (entry.bf.contains(prefix) || diff.positive.size() + diff.negative.size() >= m_threshold) {
      if (entry.bf.contains(prefix)) {
        state.addContent(ndn::Name(prefix).appendNumber(m_prefixes[prefix]));
        NDN_LOG_DEBUG("sending sync content " << prefix << " " << std::to_string(m_prefixes[prefix]));
      }
      else {
        NDN_LOG_DEBUG("Sending with empty content to send latest IBF to consumer");
      }

      // generate sync data and cancel the event
      ndn::Name syncDataName = it->first;
      m_iblt.appendToName(syncDataName);

      m_segmentPublisher.publish(it->first, syncDataName,
                                 state.wireEncode(), m_syncReplyFreshness);

      m_pendingEntries.erase(it++);
    }
    else {
      ++it;
    }
  }
}

} // namespace psync
