from flowana import Flowana
from dotenv import load_dotenv
import pyfiglet
load_dotenv()


if __name__ == '__main__':
    flowana_instance = Flowana()
    flowana_instance.run()