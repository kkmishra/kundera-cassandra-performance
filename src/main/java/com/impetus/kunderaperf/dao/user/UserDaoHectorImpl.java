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
package com.impetus.kunderaperf.dao.user;

import java.util.List;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import com.impetus.kunderaperf.dao.HectorBaseDao;
import com.impetus.kunderaperf.dto.UserDTO;

/**
 * @author amresh.singh
 * 
 */
public class UserDaoHectorImpl extends HectorBaseDao implements UserDao
{

    @Override
    public void init()
    {
        startup();
    }

    @Override
    public void insertUsers(List<UserDTO> users, boolean isBulk)
    {

        long t1 = System.currentTimeMillis();
        // Add rows
        for (int i = 0; i < users.size(); i++)
        {
            UserDTO user = users.get(i);

            insertUser(user);
        }

        // long t2 = System.currentTimeMillis();
        // System.out.println("Hector Performance: insertUsers(" + users.size()
        // + ")>>>\t" + (t2 - t1));
    }

    @Override
    public void updateUser(UserDTO userDTO)
    {
    }

    /* (non-Javadoc)
     * @see com.impetus.kunderaperf.dao.user.UserDao#findUserById(boolean, java.util.List)
     */
    @Override
    public void findUserById(boolean isBulk, List<UserDTO> users)
    {
        SliceQuery sliceQuery = prepareSliceQuery(isBulk);

        for (UserDTO u : users)
        {
            if (!isBulk)
            {
                sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
                        StringSerializer.get());
            }

            sliceQuery.setKey(u.getUserId());
            QueryResult result = sliceQuery.execute();
            assert result.get() != null;
        }

    }

    /* (non-Javadoc)
     * @see com.impetus.kunderaperf.dao.user.UserDao#findUserByUserName(java.lang.String, boolean, java.util.List)
     */
    @Override
    public void findUserByUserName(String userName, boolean isBulk, List<UserDTO> users)
    {
        IndexedSlicesQuery sliceQuery = null;

        if (isBulk)
        {
            sliceQuery = HFactory.createIndexedSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
                    StringSerializer.get());
        }

        for (UserDTO u : users)
        {
            if (!isBulk)
            {
                sliceQuery = HFactory.createIndexedSlicesQuery(keyspace, StringSerializer.get(),
                        StringSerializer.get(), StringSerializer.get());
            }
            sliceQuery.addEqualsExpression("user_nameCnt", u.getUserNameCounter());
            QueryResult result = sliceQuery.execute();
            assert result.get() != null;
        }

    }

    @Override
    public void deleteUser(String userId)
    {
    }

    public void insertUser(UserDTO user)
    {
        Mutator<String> mutator = HFactory.createMutator(keyspace, StringSerializer.get());

        mutator.addInsertion(user.getUserId(), COLUMN_FAMILY_USER,
                HFactory.createStringColumn("user_name", user.getUserName()))
                .addInsertion(user.getUserId(), COLUMN_FAMILY_USER,
                        HFactory.createStringColumn("password", user.getPassword()))
                .addInsertion(user.getUserId(), COLUMN_FAMILY_USER,
                        HFactory.createStringColumn("relation", user.getRelationshipStatus()));
        MutationResult me = mutator.execute();
        // mutator.
    }

    @Override
    public void findAll(int count)
    {
        RangeSlicesQuery keySliceQuery = HFactory.createRangeSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        keySliceQuery.setKeys("", "");
        keySliceQuery.setColumnFamily("User");
        keySliceQuery.setRowCount(count);
        QueryResult results = keySliceQuery.execute();
        assert results.get() != null;
        
    }
    
    @Override
    public void findAllByUserName(int count)
    {
        IndexedSlicesQuery idxQuery =  HFactory.createIndexedSlicesQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        idxQuery.setColumnFamily("User");
        idxQuery.addEqualsExpression("userName", "Amry");
        QueryResult results = idxQuery.execute();
        assert results.get() != null;
    }

    @Override
    public void cleanup()
    {
        shutdown();
    }

    private SliceQuery prepareSliceQuery(boolean isBulk)
    {
        SliceQuery sliceQuery = null;
        if (isBulk)
        {
            sliceQuery = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(),
                    StringSerializer.get());
        }
        return sliceQuery;
    }

}
