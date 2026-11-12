import pytest
from cwm_minio_api.load_tests.campaign.timing import sleep_checked

@pytest.mark.parametrize('advance',[1.0,1.001,20.0])
def test_check_crossing_wait_deadline_never_sleeps_negative(advance):
 now=[0.0];checks=[];sleeps=[]
 def check():checks.append(now[0]);now[0]+=advance
 def sleep(seconds):
  assert seconds>=0,'sleep interval must never be negative'
  sleeps.append(seconds);now[0]+=seconds
 sleep_checked(1,check,lambda:now[0],sleep)
 assert not sleeps
 assert len(checks)==2

def test_clock_crossing_between_loop_test_and_sleep_never_sleeps_negative():
 values=iter([0.,.999,1.001,1.002]);sleeps=[]
 def sleep(seconds):assert seconds>=0;sleeps.append(seconds)
 sleep_checked(1,clock=lambda:next(values),sleep=sleep)
 assert not sleeps
