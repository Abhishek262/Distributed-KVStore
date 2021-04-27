import json 
import pathlib
import os
from os import mkdir
from os import listdir
from os.path import isfile, join
from readerwriterlock import rwlock
from kvconstants import ErrorCodes

def hash(string) :
  hash = 5381;

  for c in string : 
      hash= ((hash << 5)+hash) + ord(c)

  return hash

class KVStore():
    maxKeyLength = 128
    maxValueLength = 512

    def __init__(self,dirname):

        pathlib.Path('storage').mkdir(parents=True, exist_ok=True) 
        self.dirname = r"storage/" + dirname 
        pathlib.Path(self.dirname).mkdir(parents=True, exist_ok=True) 
        self.lock = rwlock.RWLockFairD()

    #file location  : hash(key)-returnval.entry
    #returns val containing hash chain number
    #returns a tuple with (chain_no,value,file_name) if successful, 

    def find_entry(self,key) : 
        hashval = hash(key)
        notFound = False

        try : 
            with self.lock.gen_rlock():
                AllEntryFiles = [f for f in listdir(self.dirname) if isfile(join(self.dirname, f))]
                EntryFiles = []
                for file in AllEntryFiles : 
                    if str(hashval) in file : 
                        EntryFiles.append(file)
                    
                if(len(EntryFiles) == 0):
                    notFound = True

                if(notFound==False):
                    for file in EntryFiles : 
                        # print(file)

                        with open( self.dirname +"/" +  file,'r') as fobj : 
                            temp_dict =  json.load(fobj)
                        if(key in temp_dict.keys()):
                            return tuple([int(file.split("-")[1].split(".")[0]),temp_dict[key],file])
            

            return tuple([ErrorCodes.keyNotFound,"Not Found"]) 

        except : 
            print("Error in find_entry")
            return tuple([ErrorCodes.fileError,"File Error"]) 
            
    def contains_key(self,key) : 
        return self.find_entry(key)[0]>=0

    def KVStoreGet(self,key) : 
        ret = self.find_entry(key)
        if(ret[0]>=0) : 
            return tuple([1,ret[1]])
        else:
            return ret

    #return 1 if ok, -1 if keylength is greater, -2 if  directory does not exist, -3 if key does not exist
    def KVStoreDelCheck(self,key) : 
        if(len(str(key)) > KVStore.maxKeyLength) : 
            return ErrorCodes.errkeylen
        if(os.path.isdir(self.dirname)==False) : 
            return ErrorCodes.keyNotFound
        hashval = hash(key)
        AllEntryFiles = [f for f in listdir(self.dirname) if isfile(join(self.dirname, f))]
        exists = False
        for file in AllEntryFiles : 
            if str(hashval) in file : 
                exists = True 
        
        if(exists == True) : 
            return 1
        else : 
            return ErrorCodes.keyNotFound        

    #return 1 if ok, -1 if keylength is greater, -2 if  directory does not exist, -3 if value length exceeds max size
    def KVStorePutCheck(self,key,value) : 
        if(len(str(key)) > KVStore.maxKeyLength) : 
            return ErrorCodes.errkeylen
        if(os.path.isdir(self.dirname)==False) : 
            return ErrorCodes.fileError
        if(len(str(value)) > KVStore.maxValueLength) : 
            return ErrorCodes.errvallen

        return 1

    #file location  : hash(key)-returnval.entry
    #returns 1 if successful
    def put_entry(self,key,value) : 
        hashval = hash(key)
        existingKey = self.find_entry(key)

        with self.lock.gen_wlock():
            AllEntryFiles = [f for f in listdir(self.dirname) if isfile(join(self.dirname, f))]
            EntryFiles = []
            for file in AllEntryFiles : 
                if str(hashval) in file : 
                    EntryFiles.append(file)    

            c = len(EntryFiles)
            kvdict = {}
            kvdict[key] = value 

            fname  =self.dirname + "/" + str(hashval) + "-" + str(existingKey[0]) + ".entry"
            if(existingKey[0] >=0):
                with open(fname,'w') as fobj:
                    json.dump(kvdict,fobj)
                
            else:
                with open(self.dirname+r"/" + str(hashval) + "-" + str(c) + ".entry",'w') as fobj : 
                    json.dump(kvdict,fobj)
            
        return 1

    def KVStorePut(self,key,value) : 
        ret = self.KVStorePutCheck(key,value)
        if( ret <0) : 
            print(ret)
            return ret

        if(self.put_entry(key,value)) : 
            print("key inserted")
            return 1
        else : 
            print("Error during put")
            return -1

    #returns 1 if succesful, else returns error
    def KVStoreDelete(self,key) : 
        hashval = hash(key)
        ret = self.find_entry(key)

        try : 
            with self.lock.gen_wlock():

                delCheck = True
                del_ret = 0
                del_ret = self.KVStoreDelCheck(key)
                if(del_ret<0) : 
                    delCheck = False

                if(ret[0]!=-1 and delCheck) : 
                    fname  =self.dirname + "/" + str(hashval) + "-" + str(ret[0]) + ".entry"
                    os.remove(fname)

                if(delCheck == False):
                    return del_ret

            return 1
        
        except : 
            return ErrorCodes.fileError
        
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



# a = KVStore("dir")
# a.KVStorePut("ab","hjhjhj")
# print("get")
# print(a.KVStoreGet("ab"))
# print("del")

# print(a.KVStoreDelete("ab"))
# a.KVStoreClean()