from threading import Thread

import time

dictionary={} #dictionary that will be used to store the key-value pairs

class datastore(Thread): #datastore class containing all the funtions

    def create(self,key,value,timeLimit=0): #create method which adds key-value pair to the dictionary
        
        if key in dictionary: #checking if key already exists in the dictionary
            print('Error: The key already exists')

        else:
            if type(key)==str:    #checking if key only contains set of characters
                
                if len(key)<=32: #checking if length of characters is not greater than 32
                    
                    if len(dictionary) <= 1024 ** 3: #checking if size of dictionary exceeds 1GB or not
                        dictionaryLimit=0
                        
                    else:
                        dictionaryLimit=1
                        
                    if len(value)<=16*1024: #checking if the size of JSON object exceeds 16KB or not 
                        jsonLimit=0
                        
                    else:
                        jsonLimit=1
                        
                    if  not dictionaryLimit and not jsonLimit: #if size constraints satisfy
                        
                        if timeLimit: #checking whether Time-To-Live exists or not
                            dictionary[key]=[value,time.time()+timeLimit]
                            
                        else:
                            dictionary[key]=[value,0]
                            
                    else: #size constraints not satisfied, hence printing the appropriate message
                        
                        if dictionaryLimit:
                            print('Error:Database Memory Limit Exceeded')
                            
                        if jsonLimit:
                            print('Error: JSON Object Limit Exceeded')
                            
                else:
                    print('Error: The key size cannot be more than 32 characters')
                    
            else:
                print('Error: The key can only be a String')

    def retrieve(self,key): #retrieve method that retrieves the JSON object for corresponding key in the dictionary
        
        if key not in dictionary: #checking if key exists in dictionary or not
            print('Error:The key does not exist in the database')
            
        else:
            if dictionary[key][1]==0: #checking if there is a Time-To-Live
                print(dictionary[key][0])
                
            else:                 #checking if Time-To-Live has expired or not
                if time.time()<dictionary[key][1]:
                    print(dictionary[key][0])
                    
                else:
                    print('Error: The key has expired')

    def delete(self,key): #delete method to delete a key in dictionary
        
        if key not in dictionary: #checking if key exists in dictionary or not
            print('Error:The key does not exist in the database')
            
        else:
            if dictionary[key][1]==0:
                del dictionary[key] #deleting the key incase of no Time-To-Live
                print('The key has been successfully deleted')
                
            else:
                if time.time()>dictionary[key][1]: #if the key has expired
                    print('Error: The key has expired')
                    
                else:
                    del dictionary[key]
                    print('The key has been successfully deleted')

print('You can perform the operations by defining objects for class \'datastore\' using create(), retrieve() and delete() functions')
