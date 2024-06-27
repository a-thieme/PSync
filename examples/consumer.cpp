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

#include <PSync/consumer.hpp>

#include <ndn-cxx/face.hpp>
#include <ndn-cxx/util/logger.hpp>
#include <ndn-cxx/util/random.hpp>

#include <iostream>

NDN_LOG_INIT(examples.PartialSyncConsumerApp);

class PSyncConsumer
{
public:
  /**
   * @brief Initialize consumer and start hello process
   * @param syncPrefix should be the same as producer
   * @param nSub number of subscriptions is used for the bloom filter (subscription list) size
   */
  PSyncConsumer(const ndn::Name& syncPrefix, int nSub)
    : m_nSub(nSub)
    , m_consumer(m_face, syncPrefix, [this, nSub] {
          psync::Consumer::Options opts;
          opts.onDefaultStreamData = std::bind(&PSyncConsumer::afterReceiveHelloData, this, _1);
          opts.onUpdate = std::bind(&PSyncConsumer::processSyncUpdate, this, _1);
          opts.bfCount = nSub;
          return opts;
      } ()) {}

  void
  run()
  {
    // This starts the consumer side by sending a sync interest to the producer
    m_consumer.sendSyncInterest();
    m_face.processEvents();
  }

private:
  void
  afterReceiveHelloData(const std::vector<ndn::Name>& availSubs)
  {
    NDN_LOG_DEBUG("afterReceiveHelloData");
    std::vector<ndn::Name> sensors;
    for (const auto item : availSubs) {
      sensors.push_back(item);
    }
    std::shuffle(sensors.begin(), sensors.end(), m_rng);

    // semi-randomly subscribe to m_nSub prefixes
    for (long unsigned int i = 0; i < (long unsigned)m_nSub && i < sensors.size(); i++) {
      ndn::Name prefix = sensors.at(i);
      if (!m_consumer.isSubscribed(prefix)){
        NDN_LOG_INFO("Subscribing to: " << prefix);
        m_consumer.addSubscription(prefix);
      }
    }

    // After setting the subscription list, send the sync interest
    // The sync interest contains the subscription list
    // When new data is received for any subscribed prefix, processSyncUpdate is called
    // note: sync interests should already be scheduled
    // m_consumer.sendSyncInterest();
  }

  void
  processSyncUpdate(const std::vector<psync::MissingDataInfo>& updates)
  {
    NDN_LOG_DEBUG("processSyncUpdate ");
    for (const auto& update : updates) {
      NDN_LOG_DEBUG("prefix: " << update.prefix << " lowSeq " << update.lowSeq << " highSeq: " << update.highSeq << " face: " << update.incomingFace);
      for (uint64_t i = update.lowSeq; i <= update.highSeq; i++) {
        // Data can now be fetched using the prefix and sequence
        NDN_LOG_INFO("Update: " << update.prefix << "/" << i);
      }
    }
  }

private:
  ndn::Face m_face;

  int m_nSub;
  psync::Consumer m_consumer;

  ndn::random::RandomNumberEngine& m_rng{ndn::random::getRandomNumberEngine()};
};

int
main(int argc, char* argv[])
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <sync-prefix> <number-of-subscriptions>\n";
    return 1;
  }

  try {
    PSyncConsumer consumer(argv[1], std::stoi(argv[2]));
    consumer.run();
  }
  catch (const std::exception& e) {
    NDN_LOG_ERROR(e.what());
  }
}
