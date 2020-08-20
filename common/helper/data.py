from datetime import datetime, timedelta

DEFAULT_DATE_FORMAT = '%Y%m%d'


def add_days_to_date_id(date_id, days=1, date_fmt=DEFAULT_DATE_FORMAT):
    try:
        input_date = datetime.strptime(str(date_id), date_fmt)
        output_date = input_date + timedelta(days=days)
        return output_date.strftime(date_fmt)
    except:
        return None


def split_array(data: list, num_element_in_part=100):
    splited_data = []
    tmp_list = []
    for i in range(len(data)):
        if i != 0 and (i % num_element_in_part or i == len(data) - 1):
            tmp_list.append(data[i])
            splited_data.append(tmp_list)
            tmp_list = []
        else:
            tmp_list.append(data[i])
    return splited_data


def convert_js_dict_string_format_to_py_dict(js_format: str) -> dict:
    """
    example input data:
    { opt_out_companies: {}, site: 'bookings2', stypeid: '1', action: 'hotel', crt: '1', fbp: '1', exp1: '',
    exp2: '0', bem: '', bip: '0', exp_rmkt_test: 'global_on', ns: '0', nsc: '0',
    famem: '59636e188b466449e2bb79357a1a23556c6c795c8a1e04a321d9fc316617c780', famfn: '', fampn: '', gwcur: '0',
    gwcuc: '0', rb: '0', gst: '1', fbqs: '', sage: '744', alev: '1', atid: '204', atnm: 'Khách sạn', biz_s: '0',
    biz_p: '', bo: '1', browser: 'chrome', browser_ver: '83', bstage: '', clkid: '', emksho: '1', genis: '0', n_b: '',
    sid: 'e3e7f1cc0c060fa65fb6a901317e89d0', ui: '408561705', is_aid_mcc_level_tracked: '', partner_channel_id: '3',
    ttv: '2.14', ttv_uc: '55440.00', p1: '0.1605', hotel_class: 2, hotel_name: 'Villa Tuan Vu Da Lat', channel_id: '3',
    partner_id: '1', ai: '304142', adults: '2', book_window: '4', children: '-1', district_name: '-1',
    city_name: 'Đà Lạt', region_name: 'Lâm Đồng', country_name: 'Việt Nam', currency: 'VND', date_in: '2020-06-26',
    cul: '0', mnns: '0', date_out: '2020-06-27', dayofwk: '5', depth: '2', dest_type: 'hotel', dest_id: '2238057',
    dest_cc: 'vn', dest_ufi: '-3712045', dest_name: 'Đà Lạt, Lâm Đồng, Việt Nam', hotel_count: '0',
    hotel_id: '2238057', hotel_id_list: '', hr: '0', i1: '', i2: '', i3: '', isfd: '', isnl: '',
    language: 'vi', logged_in: '1', nights: '1', cv: '-1', ordv: '-1', rooms: '1',
    seed: 'RVNJ7iEY1owsXxF-keWCSg', srord: '-1', sub: '0', ui_a: '0', user_location: 'vn', pid: '',
    stid: '304142', gaclientid: '', tag_cdn: 'tags.tiqcdn.com' }
    :param js_format:
    :return:
    """
    js_format = js_format.strip()
    js_format = js_format[1:-1].split(',')
    return_value = dict()
    for s in js_format:
        s = s.split(':')
        try:
            value = s[1].strip().replace("'", "")
        except:
            value = ''
        return_value[s[0].strip()] = value
    return return_value


def parse_time(input_str, datetime_format=DEFAULT_DATE_FORMAT) -> datetime:
    return datetime.strptime(f'{input_str}', datetime_format)


def get_date_id(input_datetime=None) -> int:
    if input_datetime is None:
        return int(datetime.now().strftime(DEFAULT_DATE_FORMAT))
    return int(input_datetime.strftime(DEFAULT_DATE_FORMAT))


def diff_date_from_date_id(input1, input2) -> int:
    return (parse_time(input1) - parse_time(input2)).days


def minus_date_from_date_id(date_id_input, minus_days):
    """
    example: 20200104 - 3 = 20200101
    :param date_id_input:
    :param minus_days:
    :return:
    """
    return get_date_id(parse_time(date_id_input) - timedelta(days=minus_days))


def get_current_seconds():
    return int(round(datetime.now().timestamp()))