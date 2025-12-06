from make_url_and_request import make_url_and_request
from transform_data import transform_data

if __name__ == '__main__':
    transform_data(*make_url_and_request())
