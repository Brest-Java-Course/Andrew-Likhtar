package com.epam.brest.courses.service;

import com.epam.brest.courses.domain.User;

/**
 * Created by mentee-42 on 27.10.14.
 */
public class UserDataFixture {

    public static User getNewUser() {
        User user = new User();
        user.setName("name");
        user.setLogin("login");
        return user;
    }

    public static User getExistUser(long id) {
        User user = new User();
        user.setUserId(id);
        user.setName("name");
        user.setLogin("login");
        return user;
    }
}