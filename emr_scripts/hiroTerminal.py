import sys
import argparse
import time

sys.path.append("/home/hadoop/hiro_tests/")
import hiroStatAndMatchDefault as hiro

parser = argparse.ArgumentParser(description="Outputs default Hiro stat and match tests from input file")
parser.add_argument("--s3loc", help="Match file. Should be only one column")
parser.add_argument("--ticket", help="The relevant JIRA ticket. To be used for saving match_test outputs")
parser.add_argument("--test", default="all", choices=["stat","match","all"], help="Type of test desired. Choices: [stat,match,all]. Default is all")
parser.add_argument("--htype", default="md5", choices=["md5","sha1","sha2"], help="Type of hem in the original client file")
args = parser.parse_args()

if args.ticket:
    ticket = args.ticket
else:
    timeId = str(int(time.time()))
    ticket = 'noTicket_%s' %(timeId)

if args.htype:
    htype = args.htype
else:
    htype = 'md5'

clientTest = hiro.defaultTest(clientFile=args.s3loc, ticket=ticket, hem=htype)

if args.test == 'stat':
    clientTest.stat_test()
elif args.test == 'match':
    clientTest.match_test()
else:
    clientTest.stat_test()
    clientTest.match_test()