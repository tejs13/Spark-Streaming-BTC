import React, { Component } from 'react';
import { Button, Modal } from 'antd';
import { withRouter } from '../../Commons/WithRouterWrapper';
import LoginPage from '../../LoginSignup/Screens/LoginPage';

class HomeScreen extends React.Component {
    constructor(props) {
        super(props);
        this.state = {

            displayLogin: false

        }
    }
    routeChange = () => {
        console.log("button clicked")
        // this.props.navigate('/LoginPage')
    }

    handleStartClick = () => {
        console.log("botton lcikes start here")
        this.setState((prevState) => ({ displayLogin: !prevState.displayLogin }))
    }

    handleCancel = () => {
        this.setState({ displayLogin: false })
    }

    handleLoginClick = (e) => {

        console.log("Login button clicked", e)
        this.props.navigate('/selection-panel')

    }

    handleLoginFailed = (e) => {
        console.log("LOGIN FAILED", e)
    }




    render() {


        return (
            <>

                {<Modal title="" open={this.state.displayLogin} onCancel={this.handleCancel} footer={[]}
                >
                    <LoginPage
                    handleLoginClick={this.handleLoginClick}
                    handleLoginFailed={this.handleLoginFailed}


                    />
                </Modal>}



                <header className="App-header">
                    {/* <img src={logo} className="App-logo" alt="logo" /> */}
                    <h1>Welcome To Avenue Platform</h1>
                    <Button onClick={this.handleStartClick} type="primary" size='large'>Start Here</Button>
                </header>
            </>
        )
    }
}
export default withRouter(HomeScreen)



