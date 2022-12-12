import vyuha.others.ops_mis_sheeter as ms
import vyuha.others.ops_mis_consolidater as mc
import os, sys

def run_mis_sheeter():
    try:
        ms.main()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Unable to generate mis key file !')
        print(exc_type, fname, exc_tb.tb_lineno, str(e))

def run_mis_consolidater():
    try:
        opsmis = mc.OpsMIS()
        opsmis.start_pipeline()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('Unable to generate mis key file !')
        print(exc_type, fname, exc_tb.tb_lineno, str(e))