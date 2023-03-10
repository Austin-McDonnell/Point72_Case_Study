import coloredlogs
import logging
import pandas as pd
import plotly.io as pio
import plotly.express as px
import numpy as np
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Create a logger object.
logger = logging.getLogger(__name__)

# By default the install() function installs a handler on the root logger,
# this means that log messages from your code and log messages from the
# libraries that you use will all show up on the terminal.
coloredlogs.install(level='DEBUG')

# If you don't want to see log messages from libraries, you can pass a
# specific logger object to the install() function. In this case only log
# messages originating from that logger will show up on the terminal.
coloredlogs.install(level='DEBUG', logger=logger)

pio.renderers.default = 'browser'
pd.options.display.float_format = '{:,.5f}'.format
pd.options.display.max_rows = 999
pd.options.display.max_columns = 75
pd.options.plotting.backend = "plotly"
desired_width = 320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns', 80)

MAP_BOX_TOKEN = os.getenv("MAP_BOX_TOKEN")
MAP_BOX_TOKEN_PERSONAL = os.getenv("MAP_BOX_TOKEN_PERSONAL")
