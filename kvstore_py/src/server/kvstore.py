import json 
import pathlib
import os
from os import mkdir
from os import listdir
from os.path import isfile, join

def hash(string) :
  hash = 5381;

  for c in string : 
      hash= ((hash << 5)+hash) + ord(c)

  return hash

class KVStore():
    def __init__(self,dirname):

        pathlib.Path('storage').mkdir(parents=True, exist_ok=True) 
        self.dirname = r"storage/" + dirname 
        pathlib.Path(self.dirname).mkdir(parents=True, exist_ok=True) 


    #file location  : hash(key)-returnval.entry
    #returns val containing hash chain number
    #returns a tuple with (chain_no,value,file_name) if successful, (-1) or else

    def find_entry(self,key) : 
        hashval = hash(key)

        try : 
            AllEntryFiles = [f for f in listdir(self.dirname) if isfile(join(self.dirname, f))]
            EntryFiles = []
            for file in AllEntryFiles : 
                if str(hashval) in file : 
                    EntryFiles.append(file)

            for file in EntryFiles : 
                print(file)
                with open( self.dirname +"/" +  file,'r') as fobj : 
                    temp_dict =  json.load(fobj)
                if(key in temp_dict.keys()):
                    return tuple([int(file.split("-")[1].split(".")[0]),temp_dict[key],file])
            
            return tuple([-1,"Not Found"]) 

        except : 
            return -1

    def contains_key(self,key) : 
        return self.find_entry(key)[0]>=0

    #returns value or -1
    def KVStoreGet(self,key) : 
        ret = self.find_entry(key)
        if(ret[0]>=0) : 
            return ret[1]
        else:
            return -1

    #file location  : hash(key)-returnval.entry
    #returns 1 if successful
    def put_entry(self,key,value) : 
        hashval = hash(key)
        AllEntryFiles = [f for f in listdir(self.dirname) if isfile(join(self.dirname, f))]
        EntryFiles = []
        for file in AllEntryFiles : 
            if str(hashval) in file : 
                EntryFiles.append(file)    

        c = len(EntryFiles)
        kvdict = {}
        kvdict[key] = value 

        existingKey = self.find_entry(key)
        fname  =self.dirname + "/" + str(hashval) + "-" + str(existingKey[0]) + ".entry"
        if(existingKey[0] !=-1):
            with open(fname,'w') as fobj:
                json.dump(kvdict,fobj)
            
        else:
            with open(self.dirname+r"/" + str(hashval) + "-" + str(c) + ".entry",'w') as fobj : 
                json.dump(kvdict,fobj)
            
        return 1

    def KVStorePut(self,key,value) : 
        if(self.put_entry(key,value)) : 
            print("key inserted")
        else : 
            print("Error during put")

    #returns 1 if succesful, else returns -1
    def KVStoreDelete(self,key) : 
        hashval = hash(key)
        ret = self.find_entry(key)

        try : 
            if(ret[0]!=-1) : 
                fname  =self.dirname + "/" + str(hashval) + "-" + str(ret[0]) + ".entry"
                os.remove(fname)
                return 1
        
        except : 
            return -1
        
        return -1

    #returns 1 if succesful, -1 if an error occurs
    def KVStoreClean(self) : 

        try : 
            filelist = [ f for f in os.listdir(self.dirname)]
            for f in filelist:
                os.remove(os.path.join(self.dirname, f))
            os.rmdir(self.dirname)
            return 1
        except : 
            return -1


a = KVStore("dir")
a.KVStorePut("ab","hjhjhj")
# print(a.KVStoreGet("ab"))
# print(a.KVStoreDelete("abh"))
a.KVStoreClean()