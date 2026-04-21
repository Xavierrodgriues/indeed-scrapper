import sys
sys.path.insert(0, "/home/yuvii-server/Downloads/indeed-scraper")
from src.scraper import _extract_role_experience

# ---- Edge case from screenshot ----
test_jd = """
What you need:
  - 3 years of experience in DevOps or SRE
  - 7 years of experience in software engineering
  - 2 years of experience with Kubernetes
"""
print("Edge case (DevOps Engineer keyword):")
print(" Result:", _extract_role_experience(test_jd, "DevOps Engineer"))
print(" Expected: 3 years")
print()

# ---- Validate real jobs ----
import json
data = json.load(open("/home/yuvii-server/Desktop/talent-spectrum-viz/jobs.json"))
print("Real jobs:")
for job in data:
    jd = job.get("job_description", "")
    exp = _extract_role_experience(jd, "DevOps Engineer")
    print(f"  [{job['job_title']}]: {exp}")
