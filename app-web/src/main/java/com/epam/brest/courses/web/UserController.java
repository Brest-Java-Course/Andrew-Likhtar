package com.epam.brest.courses.web;

import com.epam.brest.courses.domain.User;
import com.epam.brest.courses.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@Controller
@RequestMapping("/mvc")
public class UserController {

    @Autowired
    private UserService userService;

    @RequestMapping(value = {"/"})
    public ModelAndView launchInputForm () {
        ModelAndView view = new ModelAndView("inputForm", "user", new User());
        return view;
    }

    @RequestMapping(value = {"/submitData"})
    public ModelAndView getInputForm(@RequestParam("login")String login, @RequestParam("name")String userName) {

        User user = new User();
        user.setLogin(login);
        user.setName(userName);
        Long id = userService.addUser(user);
        user.setUserId(id);
        ModelAndView view  = new ModelAndView("displayResult", "user", user);
        return view;
    }


}
