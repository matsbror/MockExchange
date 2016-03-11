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

// Poco lib includes
#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/StreamCopier.h>
#include <Poco/Path.h>
#include <Poco/URI.h>
#include <Poco/Exception.h>

// local includes
#include "json/json.h"
#include "aux_info.h"
#include "bid.h"

using namespace Poco::Net;
using namespace Poco;
using namespace std;

constexpr bool DebugOutput{ false };

struct event {
	float timestamp;
	string bidRequestId;
	string impId;
	string type; 
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



Json::Value  readConf(const std::string confFile) {
	ifstream confs(confFile);
	if (!confs.good()) {
		throw runtime_error("Could not open configuration file: " + confFile);
	}

	Json::Value configuration;
	confs >> configuration;							// read configuration json file

	return configuration;
}

// sends a win notice to the RTBkit
void sendWin(string nurl, string bidRequestId, string impId, float winPrice) {
	URI uri(nurl);

	const string host{ uri.getHost() };
	const unsigned short port{ uri.getPort() };
	const string query{ uri.getQuery() };

	vector<string> pathSegments{};
	uri.getPathSegments(pathSegments);

	HTTPClientSession session(host, port);

	std::string winNotice{ "" };

	// build the winNotice based on the nurl substitution macros
	for (auto ps : pathSegments) {
		if (ps == "${AUCTION_ID}") {
			winNotice += "/" + bidRequestId;
		}
		else if (ps == "${AUCTION_IMP_ID}") {
			winNotice += "/" + impId;
		}
		else if (ps == "${AUCTION_PRICE}") {
			winNotice += "/" + to_string(winPrice*0.9765432); // arbitrary scaling
		}
		else {
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
		//		session.reset();
		//continue;  // restart sending
	}

	HTTPResponse winNoticeResponse{};
	istream& rs = session.receiveResponse(winNoticeResponse);
	//
	//cerr << "WinNotice response:" << endl;
	//StreamCopier::copyStream(rs, coerr);

	// for every win, enter a click with some probability
	// 20% chance to get a click
	if (rand100(generator) < 20) {
		unique_lock<mutex> lck(mtx_clicks);
		event ev {0.0, bidRequestId, impId, "CLICK"};
		clicks.push_back(ev);
	}


}


// sends a PostAuction event
void sendPAEvent(string bidRequestId, string impId, string type) {
	
	// prepare session
	string uri_string = configuration["winsite"].asString();
	URI uri(uri_string);
	HTTPClientSession session(uri.getHost(), configuration["eventsport"].asInt());
	session.setKeepAlive(true);

	chrono::system_clock::time_point tp = chrono::system_clock::now();
	chrono::system_clock::duration dtn = tp.time_since_epoch();
	float ts = static_cast<float>(dtn.count() * chrono::system_clock::period::num) / chrono::system_clock::period::den;

	Json::Value clickEvent;
	clickEvent["timestamp"] = ts;
	clickEvent["bidRequestId"] = bidRequestId;
	clickEvent["impid"] = impId;
	clickEvent["type"] = type;  // CLICK or CONVERSION

	stringstream reqStream{};
	reqStream << clickEvent;
	string reqBody{ reqStream.str() };

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

	myOStream << reqBody;  // sends the body
	if (!myOStream.good()) {
		cerr << "Problem sending " << type << " event header..." << endl;
	}

	HTTPResponse clickEventResponse{};
	istream& rs = session.receiveResponse(clickEventResponse);
	//
	//std::cerr << type << " event response:" << endl;
	//StreamCopier::copyStream(rs, std::cerr);


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

		if (DebugOutput){
			if (!local_clicks.empty()) {
				cout << "Sending: " << local_clicks.size() << " click events" << endl;
				// empty entire queue

			}
			else {
				cout << "No clicks to send..." << endl;
			}
		}

		for (auto & ev : local_clicks) {
			//cerr << "Sending click on Id: " << ev.bidRequestId << endl;
			sendPAEvent(ev.bidRequestId, ev.impId, "CLICK");

			// for every click, enter a conversion with some probability
			// 10% chance to get a conversion
			if (rand100(generator) < 0) {
				unique_lock<mutex> lck(mtx_conversions);
				event ev2 {0.0, ev.bidRequestId, ev.impId, "CONVERSION"};
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

			if (DebugOutput) {
				if (!conversions.empty()) {
					cout << "Sending: " << conversions.size() << " conversion events" << endl;
					// empty entire queue

				}
				else {
					cout << "No conversions to send..." << endl;
				}
			}

			local_conversions = std::move(conversions);

			// remove all elements
			// conversions.clear(); // not needed when moved!
		}
		for (auto & ev : local_conversions) {
			//cerr << "Sending conversion on Id: " << ev.bidRequestId << endl;
			sendPAEvent(ev.bidRequestId, ev.impId, "CONVERSION");
		}
	}

}

int main(int argc, char **argv)
{
	int nrq{ 0 };
	int nrestarts{ 0 };
	Json::Value latestBr { };
	Json::Value latestBr2{};

	// read configuration file. If no command line argument is given, use rtb-adex.json
	string const conf_file{ (argc == 1 ? "rtb-adex.json" : argv[1]) };
	configuration = Json::Value(readConf(conf_file));

	read_vals(configuration["aux"]["city"].asString(), city_map);
	read_vals(configuration["aux"]["region"].asString(), region_map);
	read_vals(configuration["aux"]["upt"].asString(), user_profile_tags_map);

	const chrono::milliseconds defaultTmax{ configuration["tmax"].asInt() };

	// define and kick off the event threads
	thread clickThread(sendClicks);
//	thread conversionThread(sendConversions);

	try
	{

		// prepare session
		string uri_string = configuration["site"].asString();
		URI uri(uri_string);
		HTTPClientSession session(uri.getHost(), configuration["port"].asInt());
		session.setKeepAlive(true);


		string bid_file{ configuration["bids"].asString() };
		ifstream bids{ bid_file };
		if (!bids.good()) {
			throw runtime_error("Could not open bids file: " + bid_file);
		}

		chrono::milliseconds accumulated_time{  };

		while (!bids.eof()) {
			BidRequest br{ defaultTmax };
			bids >> br;			// Construct a bid request, one line at a time

			// filter all br that are not of format 300x50 or 300x250
			int height = { br.imp[0].banner.h };
			int width = { br.imp[0].banner.w };

			if (width != 300){
				continue;
			}
			else if ((height != 50) && (height != 250)) {
				continue;
			}

			// Now construct a JSON object out of the bid request object
			Json::Value brJson{ br.toJson() };
			latestBr2 = latestBr;
			latestBr = brJson;      // save a copy

			if (DebugOutput) {
				cout << "Request: " << nrq << ":" << endl;
				cout << brJson << endl;
			}

			stringstream reqStream{};
			reqStream << brJson;
			string reqBody{ reqStream.str() };

			HTTPRequest request(HTTPRequest::HTTP_POST, "/auctions");
			request.setKeepAlive(true);

			request.setContentType("application/json");


			request.setContentLength(reqBody.length());
			request.set("x-openrtb-version", "2.0");	// Sets openrtb version header 2.0 is used by Smaato
			request.set("x-openrtb-verbose", "1");		// request verbose reply

			bool failed{ false };
			do {

				try {
					std::ostream& myOStream = session.sendRequest(request); // sends request, returns open stream
					if (!myOStream.good()) {
						session.reset();
						continue;  // restart sending
					}


					chrono::high_resolution_clock::time_point t1 = chrono::high_resolution_clock::now();

					myOStream << reqBody;  // sends the body
					if (!myOStream.good()) {
						session.reset();
						continue;  // restart sending
					}

					if (DebugOutput) {
						// for debug output
						request.write(std::cout);
						cout << "request body: " << reqBody << endl;
					}

					HTTPResponse res;
					istream &is = session.receiveResponse(res);
					if (!is.good()) {
						session.reset();
						continue;  // restart sending
					}

					if (DebugOutput)
						cout << res.getStatus() << " " << res.getReason() << endl;

					chrono::high_resolution_clock::time_point t2 = chrono::high_resolution_clock::now();
					chrono::high_resolution_clock::duration responseTime =  t2 - t1;
					chrono::milliseconds rt{ chrono::duration_cast<chrono::milliseconds>(responseTime) };

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
							continue;  // restart sending
						}

						// debug printout
						//cerr << bid << endl;

						// 50% chance to get a win
						if (rand100(generator) < 50) {
							// find nurl, requestId, impId and winPrice
							string nurl{ bid["seatbid"][0]["bid"][0]["nurl"].asString() };
							string requestId = { bid["id"].asString() };
							string impId = { bid["seatbid"][0]["bid"][0]["impid"].asString() };
							float winPrice = { bid["seatbid"][0]["bid"][0]["price"].asFloat() * static_cast<float>(0.76) };
							sendWin(nurl, requestId, impId, winPrice);
						}

					}

					if (DebugOutput)
					{
						// print response
						if (!bid.empty())
							cout << bid;
						//StreamCopier::copyStream(is, cout);
						cout << endl;
					}

					//cout << nrq << ": It took " << rt.count() << " ms to get bid back" << endl;
					accumulated_time += rt;
					++nrq;
				} // end of try


				catch (const Exception &ex) {
					if (strncmp(ex.name(), "No message received", 19) == 0) {
						//cerr << ex.displayText() << endl;
						cerr << "restart connection..." << endl;
						++nrestarts;
						session.reset();
						continue;
					}
					else {
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

	}
	catch (Exception &ex)
	{
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