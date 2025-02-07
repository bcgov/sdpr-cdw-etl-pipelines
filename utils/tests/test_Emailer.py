import unittest
import sys
from dotenv import load_dotenv
import os 
load_dotenv()
base_dir = os.getenv('MAIN_BASE_DIR')
sys.path.append(base_dir)
from utils.emailer import Emailer

e = Emailer()
e.email(
    subject='I want to address this subject', 
    from_name='James Scott', 
    from_email='james.b.scott@gov.bc.ca', 
    to_name='James Scott', 
    to_email='james.b.scott@gov.bc.ca', 
    message='Hi',
)
