from datetime import datetime

def parse_dt(dt_str:str) ->str :
    for fmt in ("%Y-%m-%d %H:%M:%S" ,"%Y/%m/%d %H:%M:%S","%Y-%b-%d %H:%M") :
        
        try :
            dt = datetime.strptime(dt_str,fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    return None

