paragraph = [
    ["Hello", "world", "hello"],
    ["this", "is", "a", "test"],
    ["STOP", "ignore", "this", "line"],
    ["should", "not", "be", "processed"]
]

dic = {}
stop = False
for row in paragraph:
    for word in row:
        lower_word = word.lower()
        
        if lower_word == 'stop':
            stop = True
            break
        if len(lower_word) < 3:
            continue
        
        if lower_word in dic:
            dic[lower_word] += 1
        else:
            dic[lower_word] = 1
    if stop:
        break
    
print(dic)
            
    