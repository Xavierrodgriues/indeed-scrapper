import json, re

try:
    with open("/home/yuvii-server/Desktop/talent-spectrum-viz/jobs.json", "r") as f:
        data = json.load(f)
except Exception as e:
    print("Could not read jobs.json", e)
    data = []

pattern = re.compile(
    r'(?:[Mm]inimum\s+(?:of\s+)?)?((?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\+?\s*(?:to|-)?\s*(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)?\+?)\s*years?(?:\'s)?\s*(?:of\s*)?(?:hands-on\s*)?(?:professional\s*)?(?:industry\s*)?(?:related\s*)?(?:relevant\s*)?experience',
    re.IGNORECASE
)

for job in data:
    jd = job.get("job_description", "")
    match = pattern.search(jd)
    if match:
        print(f"[{job['job_title']}]: {match.group(0).strip()}")
    else:
        print(f"[{job['job_title']}]: NO MATCH")
