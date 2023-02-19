import re

text = "RAKA 08112744918 / WAHYU 085640522600 "
text_replace = text.replace("08", str('% 628'), 2)

# Use a regular expression pattern to extract both the text and the number
match = re.search(r"(\w+) % (\d+)", text_replace)
print(match)
if match:
    text_match = match.group(1)
    number_match = int(match.group(2))
    print("Text match:", text_match)
    print("Number match:", number_match)

    text_match2 = match.group(3)
    number_match2 = int(match.group(4))
    print("Text match:", text_match2)
    print("Number match:", number_match2)
else:
    print("Match not found.")
