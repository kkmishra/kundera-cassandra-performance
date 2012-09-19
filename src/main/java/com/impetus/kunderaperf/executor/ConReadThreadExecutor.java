/*******************************************************************************
 * * Copyright 2011 Impetus Infotech.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 ******************************************************************************/
package com.impetus.kunderaperf.executor;

import java.util.ArrayList;
import java.util.List;

import com.impetus.kunderaperf.dao.user.UserDao;
import com.impetus.kunderaperf.dto.UserDTO;

/**
 * @author vivek.mishra
 * 
 */
public class ConReadThreadExecutor implements Runnable
{
    private UserDao userDao;

    private int noOfRecs;

    private int counter;
    
    public Thread t;

    private String[] args;

    private String client;

    private int noOfThreads;

    public ConReadThreadExecutor(UserDao userDao, int noOfRecs, int counter, String args[], String client, int noOfThreads)
    {
        this.userDao = userDao;
        this.noOfRecs = noOfRecs;
        this.counter = counter;
        this.args = args;
        this.client = client;
        this.noOfThreads = noOfThreads;
        t = new Thread(this);
        t.start();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run()
    {
        if(args != null && args.length > 4)
        {
            String pattern = args[4];
            onRead(this.args, prepareDataSet(noOfRecs), false, client, args[2], noOfThreads,pattern);
        }
    }

    private List<UserDTO> prepareDataSet(final Integer rangeValue)
    {
        List<UserDTO> users = new ArrayList<UserDTO>();
        for (int i = 0; i < rangeValue; i++)
        {
            int key = rangeValue == 1 ? counter : i;
            UserDTO user = new UserDTO();
            user.setUserId(getString("userId_", key));
            user.setUserName(getString("Amry_", key));
            user.setPassword(getString("password_", key));
            user.setRelationshipStatus(getString("relation_", key));
            users.add(user);
        }
        return users;
    }

    private String getString(String fieldName, int key)
    {
        StringBuilder strBuild = new StringBuilder(fieldName);
        strBuild.append(key);
        return strBuild.toString();
    }
    
    
    private synchronized void onRead(String[] args, List<UserDTO> users, boolean isBulk, String client, String type,
            int noOfThreads, String pattern)
    {
        if(pattern != null && pattern.equalsIgnoreCase("rk"))
        {
            // means find by key and find All.(Single vs. Batch Read)
            long t1 = System.currentTimeMillis();
                userDao.findUserById(isBulk, users);
            long t2 = System.currentTimeMillis();
            
            String key = client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":id:s";
            if(KunderaPerformanceRunner.readProfiler.containsKey(key))
            {
                Long time = KunderaPerformanceRunner.readProfiler.get(key);
                time  = time + (t2-t1);
                KunderaPerformanceRunner.readProfiler.put(key, time);
                
            } else
            {
                KunderaPerformanceRunner.readProfiler.put(key, (t2-t1));
            }
            
            
            t1 = System.currentTimeMillis();
            
            // batch read.
            userDao.findAll(users.size());
            
            t2 = System.currentTimeMillis();
            
            key = client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":id:b";
            
            if(KunderaPerformanceRunner.readProfiler.containsKey(key))
            {
                Long time = KunderaPerformanceRunner.readProfiler.get(key);
                time  = time + (t2-t1);
                KunderaPerformanceRunner.readProfiler.put(key, time);
            } else
            {
                KunderaPerformanceRunner.readProfiler.put(key, (t2-t1));
            }
            
            
//            KunderaPerformanceRunner.readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":id:b" , (t2 - t1));
            
        } else if (pattern != null && pattern.equalsIgnoreCase("rc"))
        {
            if(!(args.length > 5))
            {
                throw new IllegalArgumentException(" invalid set of arguments!, Please provide column name as parameter");
            }
            
            String columnName = args[5];

            
            //means find by secondry index(Single vs. batch read)
            long t1 = System.currentTimeMillis();

                userDao.findUserByUserName(columnName,isBulk, users);
            long t2 = System.currentTimeMillis();
            
           String key = client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":column:s";
            if(KunderaPerformanceRunner.readProfiler.containsKey(key))
            {
                Long time = KunderaPerformanceRunner.readProfiler.get(key);
                time  = time + (t2-t1);
                KunderaPerformanceRunner.readProfiler.put(key, time);
            } else
            {
                KunderaPerformanceRunner.readProfiler.put(key, (t2-t1));
            }
            
            
//            KunderaPerformanceRunner.readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":column:s", (t2 - t1));

            t1 = System.currentTimeMillis();
            userDao.findAllByUserName(users.size());
            t2 = System.currentTimeMillis();

            key = client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":column:b";
            if(KunderaPerformanceRunner.readProfiler.containsKey(key))
            {
                Long time = KunderaPerformanceRunner.readProfiler.get(key);
                time  = time + (t2-t1);
                KunderaPerformanceRunner.readProfiler.put(key, time);
            } else
            {
                KunderaPerformanceRunner.readProfiler.put(key, (t2-t1));
            }
            

//            KunderaPerformanceRunner.readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":column:b", (t2 - t1));
            
        }
    }
}
