
def calculate_vehicle(data):
    vehicles = []
    state = {
        "B789": ["Z1", "Z2"],
        "B787": ["G-VAHH", "G-VBEL", "G-VBOW", "G-VBZZ", "G-VCRU", "G-VDIA", "G-VFAN", "G-VMAP", "G-VNEW", "G-VNYL", "G-VOOH", "G-VOWS", "G-VSPY", "G-VWHO", "G-VWOO", "G-VYUM", "G-VZIG"],
        "B744": ["G-VAST", "G-VBIG", "G-VGAL", "G-VLIP", "G-VROM", "G-VROS", "G-VROY", "G-VXLG"],
        "A330": ["G-VGBR", "G-VLUV", "G-VGEM", "G-VINE", "G-VKSS", "G-VNYC", "G-VRAY", "G-VSXY", "G-VUFO", "G-VWAG"],
        "A332": ["G-VLNM", "G-VMIK", "G-VMNK", "G-VWND"],
        "A339": ["G-VJAZ", "G-VLDY", "G-VTOM", "G-VEII"],
        "A346": ["G-VBUG", "G-VFIT", "G-VFIZ", "G-VNAP", "G-VRED", "G-VWEB", "G-VWIN", "G-VYOU"],
        "A351": ["G-VLUX", "G-VPOP", "G-VPRD", "G-VJAM", "G-VDOT", "G-VRNB", "G-VTEA", "G-VEVE", "G-VLIB", "G-VBOB"]
    }
    for index, row in data.iterrows():
        try:
            mapped_vehicle = False
            for aircraft, reg_codes in state.items():
                if row['aircraft_reg_code'] in reg_codes:
                    vehicles.append(aircraft)
                    mapped_vehicle = True
                    break
            if not mapped_vehicle:
                raise Exception('Vehicle not mapped')
        except Exception as e:
            vehicles.append(None)
    data['vehicle'] = pd.Series(data=vehicles)
    return data

def calculate_act_zero_fuel_weight_kg(data):
    data['act_zero_fuel_weight_kg'] = np.where(data['vehicle'] == 'B787', data['act_zero_fuel_weight'] * 0.454, data['act_zero_fuel_weight'])
    return data

def calculate_fuel_burn_climb(data):
    data['fuel_burn_climb1_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_climb1'] * 0.454, data['fuel_burn_climb1'])
    data['fuel_burn_climb2_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_climb2'] * 0.454, data['fuel_burn_climb2'])
    data['fuel_burn_climb3_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_climb3'] * 0.454, data['fuel_burn_climb3'])
    data['fuel_burn_climb4_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_climb4'] * 0.454, data['fuel_burn_climb4'])
    return data

def calculate_fuel_burn_cruise(data):
    data['fuel_burn_cruise1_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_cruise1'] * 0.454, data['fuel_burn_cruise1'])
    data['fuel_burn_cruise2_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_cruise2'] * 0.454, data['fuel_burn_cruise2'])
    data['fuel_burn_cruise3_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_cruise3'] * 0.454, data['fuel_burn_cruise3'])
    data['fuel_burn_cruise4_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_cruise4'] * 0.454, data['fuel_burn_cruise4'])
    return data

def calculate_fuel_burn_descent(data):
    data['fuel_burn_descent1_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_descent1'] * 0.454, data['fuel_burn_descent1'])
    data['fuel_burn_descent2_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_descent2'] * 0.454, data['fuel_burn_descent2'])
    data['fuel_burn_descent3_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_descent3'] * 0.454, data['fuel_burn_descent3'])
    data['fuel_burn_descent4_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_descent4'] * 0.454, data['fuel_burn_descent4'])
    return data

def calculate_fuel_burn_approach(data):
    data['fuel_burn_approach1_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_approach1'] * 0.454, data['fuel_burn_approach1'])
    data['fuel_burn_approach2_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_approach2'] * 0.454, data['fuel_burn_approach2'])
    data['fuel_burn_approach3_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_approach3'] * 0.454, data['fuel_burn_approach3'])
    data['fuel_burn_approach4_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_approach4'] * 0.454, data['fuel_burn_approach4'])
    return data

def calculate_fuel_burn_landing(data):
    data['fuel_burn_landing1_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_landing1'] * 0.454, data['fuel_burn_landing1'])
    data['fuel_burn_landing2_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_landing2'] * 0.454, data['fuel_burn_landing2'])
    data['fuel_burn_landing3_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_landing3'] * 0.454, data['fuel_burn_landing3'])
    data['fuel_burn_landing4_kg'] = np.where(data['vehicle'] == 'B787', data['fuel_burn_landing4'] * 0.454, data['fuel_burn_landing4'])
    return data

def calculate_planned_taxi_time(data):
    data['planned_taxi_time'] = np.where(data['planned_taxi_time'].notna(), data['planned_taxi_time'], np.where((data.taxi_in_time_sched + data.taxi_out_time_sched > 0), data.taxi_in_time_sched + data.taxi_out_time_sched, 0))
    if 'taxi_in_time_sched' not in data:
        data['planned_taxi_time'] = data['planned_taxi_time'].fillna(0).astype(float)
    return data

def calculate_taxi_in_minutes(data):
    data['taxi_in_minutes'] = np.where((data['act_blon_time'].notna()) & (data['act_landing_time'].notna()), (data.act_blon_time - data.act_landing_time).dt.total_seconds() / 60, data.taxi_in_minutes_measurement)
    return data

def calculate_taxi_out_minutes(data):
    data['taxi_out_minutes'] = np.where((data['act_takeoff_time'].notna()) & (data['act_bloff_time'].notna()) & ((data.act_takeoff_time - data.act_bloff_time).dt.total_seconds() > 0), (data.act_takeoff_time - data.act_bloff_time).dt.total_seconds() / 60, data.taxi_out_minutes_measurement)
    return data

def calculate_act_taxi_in_fuel_engines(data):
    data['act_taxi_in_fuel_engines'] = data[['fuel_burn_taxi_in1_kg', 'fuel_burn_taxi_in2_kg', 'fuel_burn_taxi_in3_kg', 'fuel_burn_taxi_in4_kg']].sum(axis=1)
    return
