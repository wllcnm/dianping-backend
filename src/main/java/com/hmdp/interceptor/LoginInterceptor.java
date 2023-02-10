package com.hmdp.interceptor;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class LoginInterceptor implements HandlerInterceptor {


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

      /*  //1.获取session
        HttpSession session = request.getSession();
        String sessionId = session.getId();
        System.out.println(sessionId);
        //2.获取session中的用户
        Object user = session.getAttribute("user");
        //3.判断用户是否存在
        if (user == null) {
            //4.不存在,拦截
            response.setStatus(401);
            return false;
        }
        //5.存在,将用户信息保存待ThreadLocal
        UserHolder.saveUser((UserDTO) user);
        return true;*/

        //1.判断是否需要拦截(threadlocal是否有用户)
        if (UserHolder.getUser() == null) {
            //如果没有就进行拦截
            response.setStatus(401);
            return false;
        }
        //如果有就放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        //移除用户
        UserHolder.removeUser();
    }
}
