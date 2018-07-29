import numpy as np
import pandas as pd
import sys
# the mock-0.3.1 dir contains testcase.py, testutils.py & mock.py
sys.path.append('../py_libs')
import athenapy

qry = athenapy.get_possible_touch_pairs_query()

df = athenapy.query("""
    SELECT *
    FROM default.datathon_transactions
    limit 100;
""")

df = athenapy.query(qry)

df
