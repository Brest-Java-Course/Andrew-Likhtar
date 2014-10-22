package com.epam.brest.courses.dao;
import com.epam.brest.courses.domain.User;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Artificial on 20.10.14.
 */
public class UserDaoImpl implements UserDao {

    private JdbcTemplate jdbcTemplate;

    public void setDataSource(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);

    }

    @Override
    public void addUser(User user) {
        jdbcTemplate.update("insert into USER (userid, login, name) values (?, ?, ?)",
                user.getUserId(), user.getLogin(), user.getName());

    }

    @Override
    public List<User> getUsers() {
        return jdbcTemplate.query("select userid, login, name from USER", new UserMapper());
    }

    @Override
    public void removeUser(Long userId) {

    }
    public class UserMapper implements RowMapper<User> {

        @Override
        public User mapRow(ResultSet rs, int i) throws SQLException {
            User user = new User();
            user.setUserId(rs.getLong("userid"));
            user.setLogin(rs.getString("login"));
            user.setName(rs.getString("name"));
            return user;
        }
    }

}
