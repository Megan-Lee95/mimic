import logging
# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s  %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)