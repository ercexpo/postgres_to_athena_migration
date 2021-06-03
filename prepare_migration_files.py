import json
import copy
from datetime import datetime, timedelta
from pathlib import Path
import traceback


def merge_dict_keys_safe(dict1, dict2, line, type_record):
    merged = dict()
    try:
        merged = merge_dict_keys(dict1, dict2)
    except Exception as e:
        print("Ops! Problem with {type_record}. Line: {line}".format(type_record=type_record,line=line))
        with open('./migration_files/error/error_standard.txt', "a", encoding="utf-8") as output:
            output.write("%s\n" % json.dumps(dict1, ensure_ascii=False))
            output.write("%s\n" % json.dumps(dict2, ensure_ascii=False))
            output.write("%d\n" % line)
            output.write("%s\n" % type_record)
            output.write("%s\n" % traceback.format_exc())
    finally:
        return merged


def merge_dict_keys(dict1, dict2):
    merged = copy.deepcopy(dict1)
    if isinstance(dict2, dict):
        for key, value in dict2.items():
            if dict1.get(key, None) is None:
                if isinstance(value, dict):
                    merged[key] = merge_dict_keys(dict1=dict(), dict2=value)
                elif isinstance(value, list):
                    merged[key] = merge_dict_keys(dict1=list(), dict2=value)
                else:
                    merged[key] = value
            else:
                merged[key] = merge_dict_keys(dict1=dict1[key], dict2=value)
    elif isinstance(dict2, list):
        for element in dict2:
            if len(merged) == 0:
                if isinstance(element, dict):
                    merged.append(merge_dict_keys(dict1=dict(), dict2=element))
                elif isinstance(element, list):
                    merged.append(merge_dict_keys(dict1=list(), dict2=element))
                else:
                    merged.append(element)
            else:
                merged[0] = merge_dict_keys(dict1=merged[0], dict2=element)
    elif dict2 is not None:
        if dict1 is None:
            merged = dict2
        elif dict2 > dict1:
            merged = dict2
    return merged


def missing_dict_keys(dict1, dict2):
    print("Returns a dictionary with the missing keys on the standard or None if None is missing")


def athena_schema(schema, timestamp_format):
    struct = ""
    for key, value in schema.items():
        struct = "{struct},\n`{key}` {value}".format(struct=struct,key=key,value=recursive_schema(value, timestamp_format))
    struct = struct.lstrip(',\n')
    return struct


def orc_schema(schema,timestamp_format):
    new_schema = recursive_schema(schema,timestamp_format).replace('`','')
    new_schema = new_schema.replace('`','').replace(' ','').replace('\n','')
    return new_schema


def recursive_schema(schema, timestamp_format):
    if isinstance(schema, dict):
        if schema == {}:
            return "struct<`dummy_key`: string>"
        else:
            struct = ""
            for key, value in schema.items():
                struct = "{struct},\n`{key}`: {value}".format(struct=struct,key=key,value=recursive_schema(value, timestamp_format))
            struct = struct.lstrip(',\n')
            return "struct<{struct}>".format(struct=struct)
    elif isinstance(schema, list):
        if len(schema) == 0:
            return "array<string>"
        else:
            return "array<{array}>".format(array=recursive_schema(schema[0], timestamp_format))
    elif isinstance(schema, bool):
        return "boolean"
    elif isinstance(schema, float):
        return "float"
    elif isinstance(schema, int):
        if schema > 2147483647:
            return "bigint"
        else:
            return "int"
    elif isinstance(schema, str):
        try:
            datetime.strptime(schema, timestamp_format)
        except ValueError as e:
            return "string"
        else:
            return "timestamp"
    elif schema is None:
        return "string"
    else:
        raise TypeError("Unknown type. It is impossible to determine the schema.")


class JSONValidator:
    def __init__(self):
        self.standard = dict()
        self.web_historian = dict()
        self.behavior_data = dict()
        self.pdk_app_event = dict()
        Path("./migration_files/orc/").mkdir(parents=True, exist_ok=True)
        Path("./migration_files/athena/").mkdir(parents=True, exist_ok=True)
        Path("./migration_files/standard/").mkdir(parents=True, exist_ok=True)
        Path("./migration_files/files/").mkdir(parents=True, exist_ok=True)
        Path("./migration_files/error/").mkdir(parents=True, exist_ok=True)

    def find_standard_in_file_and_fix(self, input_file='./passive_data_kit_datapoint.json'):
        standard_file = dict()
        standard_web_historian = dict()
        standard_behavior_data = dict()
        standard_pdk_app_event = dict()
        current_line = 0
        record_aux = dict()
        web_historian_files = dict()
        try:
            with open(input_file, "r", encoding="utf-8") as input_file:
                with open("./migration_files/files/passive_data_kit_datapoint.json", "w", encoding="utf-8") as output_file:
                    with open("./migration_files/files/web_historian_behavior_data.json", "w", encoding="utf-8") as behavior_data_file:
                        with open("./migration_files/files/pdk_app_event.json", "w", encoding="utf-8") as pdk_app_event_file:
                            for line in input_file:
                                current_line = current_line + 1
                                record_aux = json.loads(line.strip().replace('\\\\', '\\'))
                                # important for ORC conversion: https://www.threeten.org/threetenbp/apidocs/org/threeten/bp/format/DateTimeFormatterBuilder.html#appendPattern(java.lang.String)
                                record_aux['created_utc'] = record_aux['created_utc']
                                record_aux['recorded_utc'] = record_aux['recorded_utc']
                                if 'date' in record_aux['properties']:
                                    record_aux['properties']['properties_date'] = record_aux['properties'].pop('date')
                                if 'passive-data-metadata' in record_aux['properties']:
                                    record_aux['properties']['passive_data_metadata'] = record_aux['properties'].pop('passive-data-metadata')
                                    if 'generator-id' in record_aux['properties']['passive_data_metadata']:
                                        record_aux['properties']['passive_data_metadata']['generator_id'] = record_aux['properties']['passive_data_metadata'].pop('generator-id')
                                    if 'timestamp' in record_aux['properties']['passive_data_metadata']:
                                        record_aux['properties']['passive_data_metadata']['pdk_timestamp'] = record_aux['properties']['passive_data_metadata'].pop('timestamp')
                                if record_aux['generator_identifier'] == 'web-historian-behavior-metadata':
                                    if 'web-historian-server' in record_aux['properties']:
                                        record_aux['properties']['web_historian_server'] = record_aux['properties'].pop('web-historian-server')
                                    keys = list(record_aux['properties'].keys())
                                    for key in keys:
                                        if key not in ('web_historian_server', 'passive_data_metadata'):
                                            record_aux['properties']['source_info'] = record_aux['properties'].pop(key)
                                            record_aux['properties']['source_info']['source'] = key
                                    behavior_data_file.write("{}\n".format(json.dumps(record_aux, ensure_ascii=False)))
                                    standard_behavior_data = merge_dict_keys_safe(standard_behavior_data, record_aux, current_line, 'behavior_data')
                                elif record_aux['generator_identifier'] == 'web-historian':
                                    created_date = record_aux['created_utc'][0:10]
                                    created_week = datetime.strptime(created_date, '%Y-%m-%d') - timedelta(days=datetime.strptime(created_date, '%Y-%m-%d').weekday())
                                    created_week = created_week.strftime('%Y-%m-%d')
                                    if created_week not in web_historian_files:
                                        Path("./migration_files/files/web_historian/created_week={}/".format(created_week)).mkdir(parents=True, exist_ok=True)
                                        web_historian_files[created_week] = open("./migration_files/files/web_historian/created_week={}/web_historian_data.json".format(created_week), "w", encoding="utf-8")
                                    web_historian_files[created_week].write("{}\n".format(json.dumps(record_aux, ensure_ascii=False)))
                                    standard_web_historian = merge_dict_keys_safe(standard_web_historian, record_aux, current_line, 'web_historian')
                                elif record_aux['generator_identifier'] == 'pdk-app-event':
                                    pdk_app_event_file.write("{}\n".format(json.dumps(record_aux, ensure_ascii=False)))
                                    standard_pdk_app_event = merge_dict_keys_safe(standard_pdk_app_event, record_aux, current_line, 'pdk_app_event')
                                else:
                                    print('Unknown generator_identifier!')
                                    raise Exception('Very bad!')
                                output_file.write("{}\n".format(json.dumps(record_aux, ensure_ascii=False)))
                                standard_file = merge_dict_keys_safe(standard_file, record_aux, current_line, 'overall')
        except Exception as e:
            with open('./migration_files/error/error_files.txt', "a", encoding="utf-8") as output:
                print("Sorry! It did not work! Current line: %d" % current_line)
                output.write("{}\n".format(json.dumps(record_aux, ensure_ascii=False)))
                output.write("Current line: %d\n" % current_line)
                output.write("%s\n" % traceback.format_exc())
        finally:
            for web_historian_week in web_historian_files.keys():
                try:
                    web_historian_files[web_historian_week].close()
                except Exception as e:
                    print('Couldn\'t close the file {}'.format(web_historian_week))
        self.standard = standard_file
        self.web_historian = standard_web_historian
        self.behavior_data = standard_behavior_data
        self.pdk_app_event = standard_pdk_app_event

    def missing_keys(self, filename):
        standard_file = dict()
        with open(filename, "r") as input_file:
            for line in input_file:
                record_aux = json.loads(line.strip())
                standard_file = merge_dict_keys(standard_file, record_aux)
        return missing_dict_keys(self.standard, standard_file)

    def add_keys(self, new_keys):
        self.standard = merge_dict_keys(self.standard, new_keys)

    def orc_schema(self):
        with open('./migration_files/orc/web_historian_behavior_data.txt', "w") as output:
            output.write("%s" % orc_schema(self.behavior_data, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))
        with open('./migration_files/orc/web_historian.txt', "w") as output:
            output.write("%s" % orc_schema(self.web_historian, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))
        with open('./migration_files/orc/pdk_app_event.txt', "w") as output:
            output.write("%s" % orc_schema(self.pdk_app_event, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))
        with open('./migration_files/orc/passive_data_kit_datapoint.txt', "w") as output:
            output.write("%s" % orc_schema(self.standard, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))

    def athena_schema(self):
        with open('./migration_files/athena/web_historian_behavior_data.txt', "w", encoding="utf-8") as output:
            output.write("%s" % athena_schema(self.behavior_data, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))
        with open('./migration_files/athena/web_historian.txt', "w", encoding="utf-8") as output:
            output.write("%s" % athena_schema(self.web_historian, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))
        with open('./migration_files/athena/pdk_app_event.txt', "w", encoding="utf-8") as output:
            output.write("%s" % athena_schema(self.pdk_app_event, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))
        with open('./migration_files/athena/passive_data_kit_datapoint.txt', "w", encoding="utf-8") as output:
            output.write("%s" % athena_schema(self.standard, timestamp_format='%Y-%m-%d %H:%M:%S.%f'))

    def save_standard(self):
        with open('./migration_files/standard/web_historian_behavior_data.json', "w", encoding="utf-8") as output:
            output.write("%s" % json.dumps(self.behavior_data, ensure_ascii=False))
        with open('./migration_files/standard/web_historian.json', "w", encoding="utf-8") as output:
            output.write("%s" % json.dumps(self.web_historian, ensure_ascii=False))
        with open('./migration_files/standard/pdk_app_event.json', "w", encoding="utf-8") as output:
            output.write("%s" % json.dumps(self.pdk_app_event, ensure_ascii=False))
        with open('./migration_files/standard/passive_data_kit_datapoint.json', "w", encoding="utf-8") as output:
            output.write("%s" % json.dumps(self.standard, ensure_ascii=False))


def main():
    print(str(datetime.now()))
    json_validator = JSONValidator()
    json_validator.find_standard_in_file_and_fix()
    json_validator.save_standard()
    json_validator.athena_schema()
    json_validator.orc_schema()
    print(str(datetime.now()))


if __name__ == '__main__':
    main()
