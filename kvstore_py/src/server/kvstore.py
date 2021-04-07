

def hash(string) :
  hash = 5381;

  for c in string : 
      hash= ((hash << 5)+hash) + ord(c)

  return hash
