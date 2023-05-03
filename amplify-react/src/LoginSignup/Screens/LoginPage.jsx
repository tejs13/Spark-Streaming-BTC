import React, { Component } from 'react';
import '../Screens/LoginPage.css'
import { Button, Checkbox, Form, Input, Card, Modal } from 'antd';
class LoginPage extends React.Component {

    state = {}

    onFinish = (e) => {
        console.log("ON finfis", e)
        

    }

    onFinishFailed = () => {

    }


    render() {
        return (

            <>

                <Card
                    title="Login "
                    bordered={false}
                    style={{
                        width: '450px',


                    }}
                >
                    <Form
                        name="basic"
                        labelCol={{
                            span: 8,
                        }}
                        wrapperCol={{
                            span: 16,
                        }}
                        style={{
                            maxWidth: 600,
                        }}
                        initialValues={{
                            remember: true,
                        }}
                        onFinish={(e) => {this.props.handleLoginClick(e)}}
                        onFinishFailed={this.props.handleLoginFailed}
                        autoComplete="off"
                    >
                        <Form.Item
                            label="Username"
                            name="username"
                            rules={[
                                {
                                    required: true,
                                    message: 'Please input your username!',
                                },
                            ]}
                        >
                            <Input />
                        </Form.Item>

                        <Form.Item
                            label="Password"
                            name="password"
                            rules={[
                                {
                                    required: true,
                                    message: 'Please input your password!',
                                },
                            ]}
                        >
                            <Input.Password />
                        </Form.Item>

                        <Form.Item
                            name="remember"
                            valuePropName="checked"
                            wrapperCol={{
                                offset: 8,
                                span: 16,
                            }}
                        >
                            <Checkbox>Remember me</Checkbox>
                        </Form.Item>

                        <Form.Item
                            wrapperCol={{
                                offset: 8,
                                span: 16,
                            }}
                        >
                            <Button type="primary" htmlType="submit">
                                Log In
                            </Button>
                        </Form.Item>
                    </Form>
                </Card>


            </>


        )
    }



}

export default LoginPage