import configparser

def load_kafka_config(file_path="client.properties"):
    """Reads client.properties into a dict usable by confluent_kafka"""
    conf = {}
    parser = configparser.ConfigParser()

    with open(file_path) as f:
        props = "[default]\n" + f.read()

    parser.read_string(props)
    for key, value in parser["default"].items():
        conf[key] = value
    return conf