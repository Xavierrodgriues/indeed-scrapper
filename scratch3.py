import json, re

data = json.load(open("../jobs.json"))
pattern = re.compile(
    r'(?:[Mm]inimum\s+(?:of\s+)?)?((?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\+?\s*(?:to|-)?\s*(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)?\+?)\s*years?(?:\'s)?\s*(?:of\s*)?(?:hands-on\s*)?(?:professional\s*)?(?:industry\s*)?(?:related\s*)?(?:relevant\s*)?experience(?:\s+(?:in|with|using|developing|on)\s+([a-zA-Z0-9_\-\s]{1,30}))?',
    re.IGNORECASE
)

for job in data:
    jd = job.get("job_description", "")
    matches = pattern.finditer(jd)
    results = []
    for m in matches:
        val = m.group(1).strip() + " years"
        context = m.group(2)
        if context:
            # Clean up context a bit to prevent spilling over to next sentence easily
            ctx_clean = context.split('\n')[0].split('.')[0].strip()
            if ctx_clean:
                val += f" ({ctx_clean})"
        results.append(val)
        
    if results:
        seen = set()
        uniq = [x for x in results if not (x in seen or seen.add(x))]
        print(f"[{job['job_title']}]: {' | '.join(uniq)}")
    else:
        print(f"[{job['job_title']}]: NO MATCH")

