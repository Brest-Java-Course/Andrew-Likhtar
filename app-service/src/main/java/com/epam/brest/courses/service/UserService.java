package com.epam.brest.courses.service;

import com.epam.brest.courses.domain.User;

import java.util.List;

/**
 * Created by mentee-42 on 24.10.14.
 */
public interface UserService {

    public Long addUser(User user);

    public User getUserByLogin(String login);

    public User getUserById(long userId);

    public List<User> getUsers();

    public void updateUser(User user);

    public void removeUser(Long userId);

}