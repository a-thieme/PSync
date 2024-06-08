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
 */

#ifndef PSYNC_CONSUMER_HPP
#define PSYNC_CONSUMER_HPP

#include "PSync/common.hpp"
#include "PSync/detail/access-specifiers.hpp"
#include "PSync/detail/bloom-filter.hpp"

#include <ndn-cxx/face.hpp>
#include <ndn-cxx/util/random.hpp>
#include <ndn-cxx/util/scheduler.hpp>
#include <ndn-cxx/util/segment-fetcher.hpp>

#include <map>

namespace psync {

using ReceiveDefaultCallback = std::function<void(const std::vector<ndn::Name>&)>;

/**
 * @brief Consumer logic to subscribe to producer's data
 *
 * Application needs to call sendDefaultInterest to get the subscription list
 * in psync::ReceiveDefaultCallback. It can then add the desired names using addSubscription.
 * Finally application will call sendSyncInterest. If the application adds something
 * later to the subscription list then it may call sendSyncInterest again for
 * sending the next sync interest with updated IBF immediately to reduce any delay in sync data.
 * Whenever there is new data UpdateCallback will be called to notify the application.
 *
 * If consumer wakes up after a long time to sync, producer may not decode the differences
 * with its old IBF successfully and send an application nack. Upon receiving the nack,
 * consumer will send a Default again and inform the application via psync::ReceiveDefaultCallback
 * and psync::UpdateCallback.
 *
 * Currently, fetching of the data needs to be handled by the application.
 */
class Consumer
{
public:
  /**
   * @brief Constructor options.
   */
  struct Options
  {
    /// Callback to give default data back to application.
    ReceiveDefaultCallback onDefaultData = [] (const auto&) {};
    /// Callback to give sync data back to application.
    UpdateCallback onUpdate = [] (const auto&) {};
    /// Number of expected elements (subscriptions) in Bloom filter.
    uint32_t bfCount = 6;
    /// Bloom filter false positive probability.
    double bfFalsePositive = 0.001;
    /// Lifetime of default Interest.
    ndn::time::milliseconds defaultInterestLifetime = DEFAULT_INTEREST_LIFETIME;
    /// Lifetime of sync Interest.
    ndn::time::milliseconds syncInterestLifetime = SYNC_INTEREST_LIFETIME;
  };

  /**
   * @brief Constructor.
   *
   * @param face Application face.
   * @param syncPrefix Prefix to send default and sync Interests to producer.
   * @param opts Options.
   */
  Consumer(ndn::Face& face, const ndn::Name& syncPrefix, const Options& opts);

  [[deprecated]]
  Consumer(const ndn::Name& syncPrefix,
           ndn::Face& face,
           const ReceiveDefaultCallback& onReceiveDefaultData,
           const UpdateCallback& onUpdate,
           unsigned int count,
           double falsePositive = 0.001,
           ndn::time::milliseconds defaultInterestLifetime = DEFAULT_INTEREST_LIFETIME,
           ndn::time::milliseconds syncInterestLifetime = SYNC_INTEREST_LIFETIME);

  /**
   * @brief send Default interest /<sync-prefix>/DEFAULT/
   *
   * Should be called by the application whenever it wants to send a Default
   */
  void
  sendDefaultInterest();

  /**
   * @brief send sync interest /<sync-prefix>/sync/\<BF\>/\<producers-IBF\>
   *
   * Should be called after subscription list is set or updated
   */
  void
  sendSyncInterest();

  /**
   * @brief Add prefix to subscription list
   *
   * @param prefix prefix to be added to the list
   * @param seqNo the latest sequence number for the prefix received in DefaultData callback
   * @param callSyncDataCb true by default to let app know that a new sequence number is available.
   *        Usually sequence number is zero in Default data, but when it is not Consumer can
   *        notify the app. Since the app is aware of the latest sequence number by
   *        ReceiveDefaultCallback, app may choose to not let Consumer call UpdateCallback
   *        by setting this to false.
   * @return true if prefix is added, false if it is already present
   */
  bool
  addSubscription(const ndn::Name& prefix, uint64_t seqNo, bool callSyncDataCb = true);

  /**
   * @brief Remove prefix from subscription list
   *
   * @param prefix prefix to be removed from the list
   * @return true if prefix is removed, false if it is not present
   */
  bool
  removeSubscription(const ndn::Name& prefix);

  std::set<ndn::Name>
  getSubscriptionList() const
  {
    return m_subscriptionList;
  }

  bool
  isSubscribed(const ndn::Name& prefix) const
  {
    return m_subscriptionList.find(prefix) != m_subscriptionList.end();
  }

  std::optional<uint64_t>
  getSeqNo(const ndn::Name& prefix) const
  {
    auto it = m_prefixes.find(prefix);
    if (it == m_prefixes.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  /**
   * @brief Stop segment fetcher to stop the sync and free resources
   */
  void
  stop();

private:
  /**
   * @brief Get Default data from the producer
   *
   * Format: /<sync-prefix>/DEFAULT/\<BF\>/\<producer-IBF\>
   * Data content is all the prefixes the producer has.
   * We store the producer's IBF to be used in sending sync interest
   *
   * m_onReceiveDefaultData is called to let the application know
   * so that it can set the subscription list using addSubscription
   *
   * @param bufferPtr Default data content
   */
  void
  onDefaultData(const ndn::ConstBufferPtr& bufferPtr);

  /**
   * @brief Get Default data from the producer
   *
   * Format: <sync-prefix>/sync/\<BF\>/\<producers-IBF\>/\<producers-latest-IBF\>
   * Data content is all the prefixes the producer thinks the consumer doesn't have
   * have the latest update for. We update our copy of producer's IBF with the latest one.
   * Then we send another sync interest after a random jitter.
   *
   * @param bufferPtr sync data content
   */
  void
  onSyncData(const ndn::ConstBufferPtr& bufferPtr);

  void
  onDefaultStreamData(const ndn::ConstBufferPtr& bufferPtr);

PSYNC_PUBLIC_WITH_TESTS_ELSE_PRIVATE:
  ndn::Face& m_face;
  ndn::Scheduler m_scheduler;

  ndn::Name m_syncPrefix;
  ndn::Name m_defaultInterestPrefix;
  ndn::Name m_syncInterestPrefix;
  ndn::Name m_iblt;
  ndn::name::Component m_ibltEmpty;
  ndn::Name m_defaultDataName;
  ndn::Name m_syncDataName;
  uint32_t m_syncDataContentType;

  ReceiveDefaultCallback m_onReceiveDefaultData;

  // Called when new sync update is received from producer.
  UpdateCallback m_onUpdate;

  // Bloom filter is used to store application/user's subscription list.
  detail::BloomFilter m_bloomFilter;

  ndn::time::milliseconds m_defaultInterestLifetime;
  ndn::time::milliseconds m_syncInterestLifetime;

  // Store sequence number for the prefix.
  std::map<ndn::Name, uint64_t> m_prefixes;
  std::set<ndn::Name> m_subscriptionList;

  ndn::random::RandomNumberEngine& m_rng;
  std::uniform_int_distribution<> m_rangeUniformRandom;
  std::shared_ptr<ndn::SegmentFetcher> m_defaultFetcher;
  std::shared_ptr<ndn::SegmentFetcher> m_syncFetcher;
};

} // namespace psync

#endif // PSYNC_CONSUMER_HPP
