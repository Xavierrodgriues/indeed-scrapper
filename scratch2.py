import re

text = "We are looking for a dev. Minimum of 5 years of experience in AWS. Also need 3 years of experience with Docker."

exp_pattern = re.compile(
    r'(?:[Mm]inimum\s+(?:of\s+)?)?((?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\+?\s*(?:to|-)?\s*(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)?\+?)\s*years?(?:\'s)?\s*(?:of\s*)?(?:hands-on\s*)?(?:professional\s*)?(?:industry\s*)?(?:related\s*)?(?:relevant\s*)?experience(?:\s+(?:in|with|using|developing)\s+([a-zA-Z0-9_\-\s]{1,30}))?',
    re.IGNORECASE
)

matches = exp_pattern.finditer(text)
results = []
for m in matches:
    val = m.group(1).strip() + " years"
    context = m.group(2)
    if context:
        val += f" in {context.strip()}"
    results.append(val)

print(" | ".join(results))

