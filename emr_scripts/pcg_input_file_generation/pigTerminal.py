import sys
import argparse
sys.path.append("/home/hadoop")
import pcgInputGeneration as pig

parser = argparse.ArgumentParser()
parser.add_argument("--adv", help="The advertiser ID attached to the client LC tag. Should be only integers")
parser.add_argument("--bucket", help="""The client S3 bucket. e.g. "partnerName-partner-liveintent-com" """)
parser.add_argument("--start", help="""The start date for LC data collection (OPTIONAL). In the format "yyyymmdd" """)
parser.add_argument("--end", help="""The end date for LC data collection (OPTIONAL). In the format "yyyymmdd" """)

args = parser.parse_args()

if args.adv:
    adv = args.adv
    
if args.bucket:
    bucket = args.bucket
    
if args.start:
    startDate = args.start
else:
    startDate = None
    
if args.end:
    endDate = args.end
else:
    endDate = None
    
pig.pcg(advID=adv,clientBucket=bucket,startDate=startDate,endDate=endDate)

#pig.pcg(app=sys.argv[1],startDate=sys.argv[2],endDate=sys.argv[3])