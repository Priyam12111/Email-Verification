
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("email_verifier.log"),
        logging.StreamHandler()
    ]
)

log = logging.getLogger(__name__)

log.info("Logging initialized successfully.")