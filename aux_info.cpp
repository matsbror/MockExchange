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

#include "aux_info.h"
#include <fstream>

map<int, string> city_map;
map<int, string> region_map;
map<int, string> user_profile_tags_map;

int read_vals(string f, map<int, string>& m) {
	ifstream ifs(f);
	int key;
	string val;

	while (!ifs.eof()) {
		ifs >> key;
		ws(ifs);	// skip wihite space
		getline(ifs, val);
		m.emplace(key, val);
	}
	return 0;
}
