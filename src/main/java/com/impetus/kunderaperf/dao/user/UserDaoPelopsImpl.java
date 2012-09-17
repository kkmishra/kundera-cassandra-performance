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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.scale7.cassandra.pelops.exceptions.PelopsException;

import com.impetus.kunderaperf.dao.PelopsBaseDao;
import com.impetus.kunderaperf.dto.UserDTO;

/**
 * @author amresh.singh
 * 
 */
public class UserDaoPelopsImpl extends PelopsBaseDao implements UserDao
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

        for (UserDTO user : users)
        {
            insertUser(user);
        }

        // long t2 = System.currentTimeMillis();
        // System.out.println("Pelops Performance: insertUsers(" + users.size()
        // + ")>>>\t" + (t2 - t1));
    }

    public void insertUser(UserDTO user)
    {
        try
        {
            Mutator mutator = Pelops.createMutator(getPoolName());
            List<Column> columns = new ArrayList<Column>();

            long currentTime = System.currentTimeMillis();

            Column nameColumn = new Column();
            nameColumn.setName("user_name".getBytes("utf-8"));
            nameColumn.setValue(user.getUserName().getBytes("utf-8"));
            nameColumn.setTimestamp(currentTime);
            columns.add(nameColumn);

            Column passwordColumn = new Column();
            passwordColumn.setName("password".getBytes("utf-8"));
            passwordColumn.setValue(user.getPassword().getBytes("utf-8"));
            passwordColumn.setTimestamp(currentTime);
            columns.add(passwordColumn);

            Column relationColumn = new Column();
            relationColumn.setName("relation".getBytes("utf-8"));
            relationColumn.setValue(user.getRelationshipStatus().getBytes("utf-8"));
            relationColumn.setTimestamp(currentTime);
            columns.add(relationColumn);

            mutator.writeColumns(COLUMN_FAMILY, user.getUserId(), columns);
            mutator.execute(ConsistencyLevel.ONE);
        }
        catch (UnsupportedEncodingException e)
        {
            e.printStackTrace();
        }
        catch (PelopsException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void updateUser(UserDTO userDTO)
    {
    }

    @Override
    public void findUserById(boolean isBulk, List<UserDTO> users)
    {
        Selector selector = null;
        if(isBulk)
        {
            selector = Pelops.createSelector(getPoolName());
        }
        
        for(UserDTO u : users)
        {
            if(!isBulk)
            {
                selector = Pelops.createSelector(getPoolName());
            }
            
            List<Column> columns = selector.getColumnsFromRow("User", u.getUserId(), Selector.newColumnsPredicateAll(false), ConsistencyLevel.ALL);
            assert columns != null;
            
        }
    }

    @Override
    public void findAll(int count)
    {
        Selector selector = Pelops.createSelector(getPoolName());
        LinkedHashMap<Bytes, List<Column>> columns = selector.getColumnsFromRows("User", selector.newKeyRange("", "", count), false, ConsistencyLevel.ONE);
        assert columns != null;
    }
    
    @Override
    public void findAllByUserName(int count)
    {
        Selector selector = Pelops.createSelector(getPoolName());
        IndexClause idxClause = new IndexClause();
        IndexExpression expr = new IndexExpression();
        expr.setColumn_name("userName".getBytes());
        expr.setValue("Amry".getBytes());
        expr.setOp(IndexOperator.EQ);
        idxClause.addToExpressions(expr);
        LinkedHashMap<Bytes, List<Column>> results = selector.getIndexedColumns("User", idxClause, Selector.newColumnsPredicateAll(false, count), ConsistencyLevel.ONE);
        assert results != null && results.size() == count; 
    }

    @Override
    public void findUserByUserName(String userName, boolean isBulk, List<UserDTO> users)
    {
        Selector selector = null;
        if(isBulk)
        {
            selector = Pelops.createSelector(getPoolName());
        }
        
        for(UserDTO u : users)
        {
            if(!isBulk)
            {
                selector = Pelops.createSelector(getPoolName());
            }
            IndexClause idxClause = new IndexClause();
            IndexExpression expr = new IndexExpression();
            expr.setColumn_name("userNameCounter".getBytes());
            expr.setValue(u.getUserNameCounter().getBytes());
            expr.setOp(IndexOperator.EQ);
            idxClause.addToExpressions(expr);
            LinkedHashMap<Bytes, List<Column>> results = selector.getIndexedColumns("User", idxClause, Selector.newColumnsPredicateAll(false, users.size()), ConsistencyLevel.ONE);
            assert results != null && results.size() == 1;
        }
    }

    @Override
    public void deleteUser(String userId)
    {
        
    }

    @Override
    public void cleanup()
    {
        shutdown();
    }

}
