package com.epam.brest.courses.service;

import com.epam.brest.courses.dao.UserDao;
import com.epam.brest.courses.domain.User;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.util.Assert;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;


public class UserServiceImpl implements UserService {

    private static final Logger LOGGER = LogManager.getLogger();

    private UserDao userDao;

    public void setUserDao(UserDao userDao) {
        this.userDao = userDao;
    }

    @Override
    public Long addUser(User user) {
        Assert.notNull(user);
        Assert.isNull(user.getUserId());
        Assert.notNull(user.getLogin(), "User login should be specified.");
        Assert.notNull(user.getName(), "User name should be specified.");
        User existingUser = getUserByLogin(user.getLogin());
        if (existingUser != null) {
            throw new IllegalArgumentException("User is present in DB");
        }
        return userDao.addUser(user);
    }


    @Override
    public User getUserByLogin(String login) {
        LOGGER.debug("getUserByLogin({}) ", login);
        User user = null;
        try {
            user = userDao.getUserByLogin(login);
        } catch (EmptyResultDataAccessException e) {
            LOGGER.error("getUserByLogin({}) ", login);
        }
        return user;
    }

    @Override
    public User getUserById(long userId) {
        throw new NotImplementedException();
    }

    @Override
    public List<User> getUsers() {
        throw new NotImplementedException();
    }

    @Override
    public void updateUser(User user) {

    }

    @Override
    public void removeUser(Long userId) {

    }
}