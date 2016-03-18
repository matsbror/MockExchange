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


#pragma once

#include<string>
#include<vector>
#include<chrono>
#include<iostream>

#include "json/json.h"

struct BannerObject {
	int w;			// width (in pixels)
	int h;			// height
	std::string id;

public:
	BannerObject(int width, int height, std::string id = "")
		: w{ width }, h{ height }, id{ id }
	{
	}
};


struct ImpressionObject {
	const std::string id;
	const BannerObject banner;
	const float bidfloor;
	//	const VideoObject video;

public:
	// For banners
	ImpressionObject(std::string tid, BannerObject tbanner, float tbidfloor = 0.0)
		: id{ tid }, banner{ tbanner }, bidfloor{tbidfloor}
	{
	};


};

class SiteObject {

};

class AppObject {

};

class GeoObject {
	float lat;
	float lon;
	std::string country;		// using ISO-3166-1, alpha 3
	std::string region;		// using 3166-2
	std::string metro;

public:
	GeoObject(std::string &&country, std::string &&region, std::string &&metro)
		: country{ country }, region{ region }, metro{ metro }
	{

	}

};

struct DeviceObject {
	int dnt;		// Do not track
	std::string ua;		// User agent 
	std::string ip;		// ip address
};

class UserObject {

};

// structure to hold a bid request
struct BidRequest {

	std::string id;				// bid request id
	std::vector<ImpressionObject> imp;		// array of impression objects
	SiteObject site;
	AppObject app;
	DeviceObject device;
        std::vector<std::string> badv;
        std::vector<std::string> bcat;
	UserObject user;
	int at;					// auction type 1 = first price auction, 2 = second price auction
	const std::chrono::milliseconds tmax;				// max time bidder has to reply (in ms)
	std::vector<std::string> wseat;			// array of buyes seats allowed to bid

							// the following are not really part of the bid, but kept for determining	if a bid reply wins or not
	float bidding_price;	// not used in bid requests, but to determine if a bid wins or not
	float paying_price;		// if bid response is above paying_price it is considered a win (USD)

							// other optional parameters in a bid request according to OpenRTB ommitted for now

							// make an empty request
	BidRequest()
		: id{}, imp{}, tmax{}, app{  }, device{  }, at{}
	{
	}
	BidRequest(std::chrono::milliseconds ttmax = std::chrono::milliseconds(100))
		: id{}, imp{}, tmax{ ttmax }, app{  }, device{  }, at{}
	{
	}

	~BidRequest() {
	}

	// Build a Json bid request object out of the C++ object
	Json::Value toJson()
	{
		Json::Value br_root;

		br_root["id"] = id;
		for (auto x : imp) {
			Json::Value imp_inst{};
			imp_inst["id"] = x.id;
			imp_inst["bidfloor"] = x.bidfloor;
			imp_inst["banner"]["w"] = x.banner.w;
			imp_inst["banner"]["h"] = x.banner.h;
			imp_inst["banner"]["mimes"].append(Json::Value("image/gif"));
			br_root["imp"].append(imp_inst);
		}
		Json::Value dev_inst{};
		dev_inst["dnt"] = device.dnt;
		dev_inst["ua"] = device.ua;
		dev_inst["ip"] = device.ip;
		br_root["device"] = dev_inst;
                for (auto bc : bcat)
                    br_root["bcat"].append(Json::Value(bc));
                for (auto bv : badv)
                    br_root["badv"].append(Json::Value(bv));
                
		return br_root;
	}

};


// read in the bid request from impression file from ipinyou data season 3
std::istream& operator>>(std::istream &bids, BidRequest& br)
{

	std::string dummy{};

	// Use getline consistently 
	std::string brid{};			// Bid request id
	getline(bids, brid, '\t');

	getline(bids, dummy, '\t');	// skip timestamp
	getline(bids, dummy, '\t');	// skip log type
	getline(bids, dummy, '\t');	// skip iPinYou id as well

	std::string ua{};			// user agent information
	getline(bids, ua, '\t');	// read user agent information

	std::string ipaddr{};		// user's ip address
	getline(bids, ipaddr, '\t');		// read the ip address
	if (ipaddr.back() == '*')
		ipaddr.back() = '0';		// Change * to 0 if the last character was a *

	std::string region_id{};
	getline(bids, region_id, '\t'); // read region number
	std::string region{ region_map[stoi(region_id)] };
	std::string city_id{};
	getline(bids, city_id, '\t');   // read city id
	std::string city{ city_map[stoi(city_id)] };

	std::string adexId_str{};
	getline(bids, adexId_str, '\t');	// read adexchange id
	int adexId{ stoi(adexId_str) };

	getline(bids, dummy, '\t');	// skip domain
	getline(bids, dummy, '\t');	// skip url
	getline(bids, dummy, '\t');	// skip anon url id
	getline(bids, dummy, '\t');	// skip ad slot id

	std::string ad_slot_width{};
	getline(bids, ad_slot_width, '\t');			// read ad slot width
	std::string ad_slot_height{};
	getline(bids, ad_slot_height, '\t');

	getline(bids, dummy, '\t');	// skip ad slot visibility 
	getline(bids, dummy, '\t');	// skip ad slot format

	std::string ad_slot_floor_price{};
	getline(bids, ad_slot_floor_price, '\t');

	getline(bids, dummy, '\t');	// skip creative id

	std::string bidding_price{};
	getline(bids, bidding_price, '\t');		// read bidding price from log

	std::string paying_price{};
	getline(bids, paying_price, '\t');		// read paying price from log

	getline(bids, dummy);		// skip rest of line
	ws(bids);					// and get rid of some white spaces while we're at it

	BannerObject bannerObj{ stoi(ad_slot_width), stoi(ad_slot_height) };
	float bf = stof(ad_slot_floor_price) / 10;
	if (bf == 0) {
		bf = 0.1;
	}
	ImpressionObject impObj{ "1", bannerObj,  bf};
	// Let's now construct a BidRequest object
	br.id = brid;
	br.imp.push_back(impObj);
	br.device = { 0, ua, ipaddr };
	br.bidding_price = stof(bidding_price) / 10;
	br.paying_price = stof(paying_price) / 10;


	return bids;
}
