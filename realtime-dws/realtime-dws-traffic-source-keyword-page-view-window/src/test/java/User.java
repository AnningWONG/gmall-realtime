import java.math.BigInteger;

/**
 * ClassName: User
 * Package: PACKAGE_NAME
 * Description:
 *      POJO （JDBC API用）
 * @Author Wang Anning
 * @Create 2024/4/25 23:16
 * @Version 1.0
 */
public class User {
    public Integer siteid;
    public Integer citycode;
    public String username;
    public Long pv;

    public User() {
    }

    public User(Integer siteid, Integer citycode, String username, Long pv) {
        this.siteid = siteid;
        this.citycode = citycode;
        this.username = username;
        this.pv = pv;
    }

    public Integer getSiteid() {
        return siteid;
    }

    public void setSiteid(Integer siteid) {
        this.siteid = siteid;
    }

    public Integer getCitycode() {
        return citycode;
    }

    public void setCitycode(Integer citycode) {
        this.citycode = citycode;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Long getPv() {
        return pv;
    }

    public void setPv(Long pv) {
        this.pv = pv;
    }
}
