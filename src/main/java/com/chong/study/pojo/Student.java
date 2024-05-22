package com.chong.study.pojo;

public class Student {
    private int id;
    private final String name;
    private final float chinese;
    private final float english;
    private final float math;
    private float total = 0;

    public Student(int id, String name, float chinese, float english, float math) {
        this.id = id;
        this.name = name;
        this.chinese = chinese;
        this.english = english;
        this.math = math;
    }

    public Student(int id, String name, float chinese, float english, float math, float  total) {
        this.id = id;
        this.name = name;
        this.chinese = chinese;
        this.english = english;
        this.math = math;
        this.total = total;
    }

    public Student(String name, float chinese, float english, float math) {
        this.name = name;
        this.chinese = chinese;
        this.english = english;
        this.math = math;
    }

    public void setTotal(float total) {
        this.total = total;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public float getChinese() {
        return chinese;
    }

    public float getEnglish() {
        return english;
    }

    public float getMath() {
        return math;
    }

    public float getTotal() {
        return total;
    }

    @Override
    public String toString() {
        return "Student [id=" + id + ", name=" + name + ", chinese=" + chinese + ", english=" + english + ", math="
                + math + ", total=" + total + "]";
    }

}
