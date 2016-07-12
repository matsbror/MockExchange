//   Copyright 2016 Mats Brorsson, OLA Mobile S.a.r.l
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

// stdlib includes
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <exception>
#include <chrono>
#include <random>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <cassert>
#include <unistd.h>

// Poco lib includes
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/IPAddress.h>
#include "Poco/Net/NetException.h"
#include <Poco/StreamCopier.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Poco/Exception.h>
#include <Poco/Logger.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/FileChannel.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/SplitterChannel.h>
#include <Poco/AutoPtr.h>


// local includes
#include "json/json.h"
#include "aux_info.h"
#include "bid.h"

using namespace Poco::Net;
using namespace Poco;
using Poco::Logger;
using Poco::FormattingChannel;
using Poco::PatternFormatter;
using Poco::FileChannel;
using Poco::ConsoleChannel;
using Poco::SplitterChannel;
using Poco::AutoPtr;

using namespace std;


struct event {
    float timestamp;
    string bidRequestId;
    string impId;
    string type;
    Logger & logger;
};

// Shared queue for click events
condition_variable cv_clicks;
mutex mtx_clicks;
vector<event> clicks;

// Shared queue for conversion events
condition_variable cv_conversions;
mutex mtx_conversions;
vector<event> conversions;

Json::Value configuration{};


std::default_random_engine generator;
std::uniform_int_distribution<int> rand100(0, 100);

Json::Value readConf(const std::string confFile) {
    ifstream confs(confFile);
    if (!confs.good()) {
        throw runtime_error("Could not open configuration file: " + confFile);
    }

    Json::Value configuration;
    confs >> configuration; // read configuration json file

    return configuration;
}

// sends a win notice to the RTBkit

void sendWin(Logger & logger, string nurl, string bidRequestId, string impId, float winPrice) {
    URI uri(nurl);

    const string host{uri.getHost()};
    const unsigned short port{static_cast<unsigned short> (configuration["winport"].asInt())};
    const string query{uri.getQuery()};

    vector<string> pathSegments{};
    uri.getPathSegments(pathSegments);


    try {
        HTTPClientSession session(host, port);

        if (configuration["wnstyle"].asString() == "smaato") {


            std::string winNotice{""};

            // build the winNotice based on the nurl substitution macros
            for (auto ps : pathSegments) {
                if (ps == "${AUCTION_ID}") {
                    winNotice += "/" + bidRequestId;
                } else if (ps == "${AUCTION_IMP_ID}") {
                    winNotice += "/" + impId;
                } else if (ps == "${AUCTION_PRICE}") {
                    winNotice += "/" + to_string(winPrice * 0.9765432); // arbitrary scaling
                } else {
                    winNotice += "/" + ps;
                }
            }

            // debug
            //cerr << "winNotice: " << winNotice << endl;

            HTTPRequest request(HTTPRequest::HTTP_GET, winNotice);
            //	request.setKeepAlive(true);

            std::ostream& myOStream = session.sendRequest(request); // sends request, returns open stream

            if (!myOStream.good()) {
                cerr << "Problem sending win notice header..." << endl;
            }

        } else {
            // It's rtbkit style, send the win notice as POST JSON
            chrono::system_clock::time_point tp = chrono::system_clock::now();
            int ts = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();

            Json::Value wn;
            wn["timestamp"] = static_cast<float>(ts);
            wn["bidRequestId"] = bidRequestId;
            wn["impid"] = impId;
            wn["price"] = winPrice * 0.9765432;

            stringstream reqStream{};
            reqStream << wn;
            string reqBody{reqStream.str()};

            HTTPRequest request(HTTPRequest::HTTP_POST, "/wins");
            request.setKeepAlive(true);

            request.setContentType("application/json");

            request.setContentLength(reqBody.length());

            // debug
            //cerr << "host: " << host << ", port: " << port << endl;
            //cerr << "winNotice: " << wn << endl;

            std::ostream& myOStream = session.sendRequest(request); // sends request, returns open stream

            if (!myOStream.good()) {
                cerr << "Problem sending event header..." << endl;
            }

            myOStream << reqBody; // sends the body
            if (!myOStream.good()) {
                cerr << "Problem sending event body..." << endl;
            }

        } // else wnStyle rtbkit



        logger.information("WIN\t" + bidRequestId);

        HTTPResponse winNoticeResponse{};
        istream& rs = session.receiveResponse(winNoticeResponse);
        // debug
        //cerr << "WinNotice response:" << endl;
        //StreamCopier::copyStream(rs, cerr);
    } catch (const Poco::Net::HostNotFoundException &noHostEx) {
        std::cerr << "Host Not found: " << host << ", port: " << port << std::endl;
        exit(-1);
    }

    // for every win, enter a click with some probability
    // 20% chance to get a click
    if (rand100(generator) < 20) {
        unique_lock<mutex> lck(mtx_clicks);
        event ev{0.0, bidRequestId, impId, "CLICK", logger};
        clicks.push_back(ev);
    }


}


// sends a PostAuction event

void sendPAEvent(Logger & logger, string bidRequestId, string impId, string type) {

    // prepare session
    string uri_string = configuration["winsite"].asString();
    URI uri(uri_string);
    string host { uri.getHost() };
    unsigned short port { static_cast<unsigned short>(configuration["eventsport"].asInt()) };

    try {

        HTTPClientSession session(host, port );
        session.setKeepAlive(true);

        chrono::system_clock::time_point tp = chrono::system_clock::now();
        int ts = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
        
        Json::Value clickEvent;
        clickEvent["timestamp"] = static_cast<float>(ts);
        clickEvent["bidRequestId"] = bidRequestId;
        clickEvent["impid"] = impId;
        clickEvent["type"] = type; // CLICK or CONVERSION

        stringstream reqStream{};
        reqStream << clickEvent;
        string reqBody{reqStream.str()};

        HTTPRequest request(HTTPRequest::HTTP_POST, "/");
        request.setKeepAlive(true);

        request.setContentType("application/json");

        request.setContentLength(reqBody.length());

        // debug
        //cerr << "clickEvent: " << clickEvent << endl;

        std::ostream& myOStream = session.sendRequest(request); // sends request, returns open stream

        if (!myOStream.good()) {
            cerr << "Problem sending " << type << " event header..." << endl;
        }

        myOStream << reqBody; // sends the body
        if (!myOStream.good()) {
            cerr << "Problem sending " << type << " event body..." << endl;
        }

        logger.information(type + "\t" + bidRequestId);

        HTTPResponse clickEventResponse{};
        istream& rs = session.receiveResponse(clickEventResponse);
        //
        //std::cerr << type << " event response:" << endl;
        //StreamCopier::copyStream(rs, std::cerr);
    } catch (const Poco::Net::HostNotFoundException &noHostEx) {
        std::cerr << "SendPAevent: Host Not found: " << host << ", port: " << port << std::endl;
        exit(-1);
    }


}


// thread simulating sending clicks

void sendClicks() {

    vector<event> local_clicks{};

    cerr << "Starting sendClicks thread" << endl;

    while (true) {
        {
            unique_lock<mutex> lck(mtx_clicks);
            cv_clicks.wait_for(lck, chrono::duration<int>(10)); // wait 10 seconds

            local_clicks = std::move(clicks);

            // remove all elements
            // clicks.clear(); // not needed when moved!
        }

        assert(clicks.empty());

        sleep(5); // sleep 5 seconds to make sure the win comes before the click

        if (false /* change to true for debug output */) {
            if (!local_clicks.empty()) {
                cout << "Sending: " << local_clicks.size() << " click events" << endl;
                // empty entire queue

            } else {
                cout << "No clicks to send..." << endl;
            }
        }

        for (auto & ev : local_clicks) {
            //cerr << "Sending click on Id: " << ev.bidRequestId << endl;
            sendPAEvent(ev.logger, ev.bidRequestId, ev.impId, "CLICK");

            // for every click, enter a conversion with some probability
            // 10% chance to get a conversion
            if (rand100(generator) < 0) {
                unique_lock<mutex> lck(mtx_conversions);
                event ev2{0.0, ev.bidRequestId, ev.impId, "CONVERSION", ev.logger};
                conversions.push_back(ev2);
            }
        }
    }
}

// thread simulating sending conversions

void sendConversions() {
    vector<event> local_conversions{};

    cerr << "Starting sendConversion thread" << endl;

    while (true) {
        {
            unique_lock<mutex> lck(mtx_conversions);
            cv_conversions.wait_for(lck, chrono::duration<int>(95)); // wait 95 seconds

            if (false /* change to true for debug output */) {
                if (!conversions.empty()) {
                    cout << "Sending: " << conversions.size() << " conversion events" << endl;
                    // empty entire queue

                } else {
                    cout << "No conversions to send..." << endl;
                }
            }

            local_conversions = std::move(conversions);

            // remove all elements
            // conversions.clear(); // not needed when moved!
        }
        for (auto & ev : local_conversions) {
            //cerr << "Sending conversion on Id: " << ev.bidRequestId << endl;
            sendPAEvent(ev.logger, ev.bidRequestId, ev.impId, "CONVERSION");
        }
    }

}

int main(int argc, char **argv) {
    int nrq{0};
    int nrestarts{0};
    Json::Value latestBr{};
    Json::Value latestBr2{};

    // read configuration file. If no command line argument is given, use rtb-adex.json
    string const conf_file{(argc == 1 ? "rtb-adex.json" : argv[1])};
    configuration = Json::Value(readConf(conf_file));

    string lf{configuration["logfile"].asString()};
    std::cerr << "Setting log file to " + lf << std::endl;

    AutoPtr<FileChannel> pChannel(new FileChannel);

    pChannel->setProperty("path", lf);
    pChannel->setProperty("rotation", "1 M");
    pChannel->setProperty("archive", "timestamp");
    AutoPtr<ConsoleChannel> pcChannel(new ConsoleChannel);
    AutoPtr<PatternFormatter> pPF(new PatternFormatter);
    pPF->setProperty("pattern", "%Y-%m-%d %H:%M:%S %s: %t");
    AutoPtr<FormattingChannel> pFC(new FormattingChannel(pPF, pChannel));
    AutoPtr<SplitterChannel> pSplitter(new SplitterChannel);

    // Uncomment the line below if you want log messages go to console in addition to file.
    // pSplitter->addChannel(pcChannel);
    pSplitter->addChannel(pFC);

    Logger::root().setChannel(pSplitter);
    Logger& logger = Logger::get("IDGeneratorLogger"); // inherits root channel

    logger.information("******** NEW LOG ENTRY *********");


    read_vals(configuration["aux"]["city"].asString(), city_map);
    read_vals(configuration["aux"]["region"].asString(), region_map);
    read_vals(configuration["aux"]["upt"].asString(), user_profile_tags_map);

    const chrono::milliseconds defaultTmax{configuration["tmax"].asInt()};

    // define and kick off the event threads
    thread clickThread(sendClicks);
    //	thread conversionThread(sendConversions);

    try {

        // prepare session
        string uri_string = configuration["site"].asString();
        URI uri(uri_string);
        HTTPClientSession session(uri.getHost(), configuration["port"].asInt());
        session.setKeepAlive(true);


        string bid_file{configuration["bids"].asString()};
        ifstream bids{bid_file};
        if (!bids.good()) {
            throw runtime_error("Could not open bids file: " + bid_file);
        }

        chrono::milliseconds accumulated_time{};

        while (!bids.eof()) {
            BidRequest br{defaultTmax};
            bids >> br; // Construct a bid request, one line at a time

            // filter all br that are not of format 300x50 or 300x250
            int height = {br.imp[0].banner.h};
            int width = {br.imp[0].banner.w};

            if (width != 300) {
                continue;
            } else if ((height != 50) && (height != 250)) {
                continue;
            }

            // set blocked categories 
            br.bcat.push_back("IAB22");
            // set fake operator
            br.ext.carrierName = "personal";

            // Now construct a JSON object out of the bid request object
            Json::Value brJson{br.toJson()};
            latestBr2 = latestBr;
            latestBr = brJson; // save a copy

            // debug
            //cerr << "Request: " << nrq << ":" << endl;
            //cerr << brJson << endl;

            stringstream reqStream{};
            reqStream << brJson;
            string reqBody{reqStream.str()};

            HTTPRequest request(HTTPRequest::HTTP_POST, "/auctions");
            request.setKeepAlive(true);

            request.setContentType("application/json");


            request.setContentLength(reqBody.length());
            request.set("x-openrtb-version", "2.0"); // Sets openrtb version header 2.0 is used by Smaato
            request.set("x-openrtb-verbose", "1"); // request verbose reply

            bool failed{false};
            do {

                try {
                    std::ostream& myOStream = session.sendRequest(request); // sends request, returns open stream
                    if (!myOStream.good()) {
                        session.reset();
                        continue; // restart sending
                    }


                    chrono::high_resolution_clock::time_point t1 = chrono::high_resolution_clock::now();

                    myOStream << reqBody; // sends the body
                    if (!myOStream.good()) {
                        session.reset();
                        continue; // restart sending
                    }

                    if (false /* change to true for debug output */) {
                        // for debug output
                        request.write(std::cout);
                        cout << "request body: " << reqBody << endl;
                    }

                    logger.information("BR\t" + brJson["id"].asString());

                    HTTPResponse res;
                    istream &is = session.receiveResponse(res);
                    if (!is.good()) {
                        session.reset();
                        continue; // restart sending
                    }

                    if (false /* change to true for debug output */)
                        cout << res.getStatus() << " " << res.getReason() << endl;

                    chrono::high_resolution_clock::time_point t2 = chrono::high_resolution_clock::now();
                    chrono::high_resolution_clock::duration responseTime = t2 - t1;
                    chrono::milliseconds rt{chrono::duration_cast<chrono::milliseconds>(responseTime)};

                    if (rt > defaultTmax) {
                        cout << "Bid id: " << br.id << " arrived too late: " << rt.count() << " ms." << endl;
                    }

                    // Get respons to Json Value
                    Json::Value bid{};
                    if (res.getStatus() != 204) {
                        // This is a good bid, parse it and determine if it's a win
                        is >> bid;

                        if (!is.good()) {
                            session.reset();
                            continue; // restart sending
                        }

                        // debug printout
                        //cerr << bid << endl;
                        logger.information("BID\t" + bid["id"].asString());

                        // 50% chance to get a win
                        if (rand100(generator) < 50) {
                            // find nurl, requestId, impId and winPrice
                            string nurl{bid["seatbid"][0]["bid"][0]["nurl"].asString()};
                            string requestId = {bid["id"].asString()};
                            string impId = {bid["seatbid"][0]["bid"][0]["impid"].asString()};
                            float winPrice = {bid["seatbid"][0]["bid"][0]["price"].asFloat() * static_cast<float> (0.76)};
                            sendWin(logger, nurl, requestId, impId, winPrice);
                        }

                    }

                    if (false /* change to true for debug output */) {
                        // print response
                        if (!bid.empty())
                            cout << bid;
                        //StreamCopier::copyStream(is, cout);
                        cout << endl;
                    }

                    //cout << nrq << ": It took " << rt.count() << " ms to get bid back" << endl;
                    accumulated_time += rt;
                    ++nrq;
                    failed = false;
                }// end of try
 catch (const Poco::Net::NoMessageException &noMsgEx) {
                    std::cerr << "No message received. Restart connection..." << std::endl;
                    session.reset();
                    continue;
                } catch (const Poco::Net::ConnectionResetException &netEx) {
                    std::string errstr = {"Socket Error : " + netEx.displayText()};
                    std::cerr << errstr << std::endl;
                    break;
                } catch (const Poco::Net::ConnectionRefusedException &netEx) {

                    std::string errstr = {"Socket Error : " + netEx.displayText()};
                    std::cerr << errstr << std::endl;
                    break;
                } catch (const Poco::Net::ConnectionAbortedException &netEx) {
                    std::string errstr = {"Socket Error : " + netEx.displayText()};
                    std::cerr << "Msg forward failed: " << errstr << std::endl;
                    break;
                }
 catch (const Exception &ex) {
                    if (strncmp(ex.name(), "No message received", 19) == 0) {
                        //cerr << ex.displayText() << endl;
                        cerr << "restart connection..." << endl;
                        ++nrestarts;
                        session.reset();
                        continue;
                    } else {
                        cerr << ex.displayText() << endl;
                        cout << "Time for bid reply on average: " << accumulated_time.count() / nrq << " ms over " << nrq << " bid requests sent." << endl;
                        cout << "Number of restarts:" << nrestarts << endl;
                        return -1;
                    }
                }

            } while (failed);

            //session.reset();

        }


        cout << "Time for bid reply on average: " << accumulated_time.count() / nrq << " ms over " << nrq << " bid requests sent." << endl;
        cout << "My work is done..." << endl;

    } catch (Exception &ex) {
        cerr << "Sent " << nrq << " bid requests before failing..." << endl;
        cerr << "Bid request before failed:" << endl;
        cerr << latestBr2 << endl;
        cerr << "Failed bid request:" << endl;
        cerr << latestBr << endl;

        cerr << ex.displayText() << endl;
        return -1;
    }

    return 0;
}
