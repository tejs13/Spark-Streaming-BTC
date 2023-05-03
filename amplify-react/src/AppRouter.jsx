import React, { Component } from 'react';
import { BrowserRouter as Router, Routes, Route, Switch } from "react-router-dom";
import Dashboard from './Dashboard/Screens/Dashboard';
import HomeScreen from './HomeScreen/Screens/HomeScreen';
import LoginPage from './LoginSignup/Screens/LoginPage';
import SelectionPanel from './LoginSignup/Screens/SelectionPanel';


class AppRouters extends React.Component {
    state = {}


    render() {
        return (
            <Routes>
                <Route exact path="/" element={<HomeScreen />} />

                <Route path='/Dashboard' element={<Dashboard />} />

                <Route path='/LoginPage' element={<LoginPage />} />

                <Route path='/selection-panel' element={<SelectionPanel/>} />

            </Routes>

        )
    }
}

export default AppRouters