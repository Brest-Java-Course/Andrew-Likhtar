package com.epam.brest.courses.dao;
import com.epam.brest.courses.domain.User;
import java.util.List;

/**
 * Created by Artificial on 20.10.14.
 */
public interface UserDao {
    public Long addUser(User user);
    public List <User> getUsers();
    public void removeUser(Long userId);
    public User getUserById(long userId);
    public User getUserByLogin(String login);
    public void updateUser(User user);


}
