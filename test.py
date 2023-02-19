import re

text = "AGUS NURCAHYONO (082254797789 / 08115231644) | RENDY ADRIANO S (085294467150) | BAYU (085751821326) PET ATM | PIC KCP MEDAWAI Akbar budi setyawan  082150433031 ->AGUS SALIM (085335013731)"
text_replace = text.replace("|", str('/'), 1)
words = re.split('/', text_replace)
result_grp = re.search(r'[a-zA-Z\s]+', words[0])
name_pic = result_grp.group()
print(words)
