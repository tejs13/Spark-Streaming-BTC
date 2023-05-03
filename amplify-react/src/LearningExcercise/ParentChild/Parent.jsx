import React, {Component} from 'react';

import Child from './Child';
import { Button } from 'antd';



class Parent extends React.Component{


    state = {
        clickCount : 0
    }

    hanldeClick = (e) => {
        console.log(e);
        this.setState((prevState, props) => ({clickCount : prevState.clickCount + 1}), ()=>{console.log(this.state.clickCount)})
    }

    getClickFromChild = (cnt) => {
        console.log(cnt + "FROM CHILD");
        if (cnt % 5 ==0 && cnt > 1) {
            this.setState({clickCount : 0})
        }

    }



    render() {
        return(
            <>
            <h1>State Down and Lifting State Up </h1>
            <p>Parent Component</p>
            <Button type='primary' onClick={this.hanldeClick}> Parent</Button>
            <Child clickCount={this.state.clickCount} getClick={this.getClickFromChild}></Child>
            
            </>
        )
    }
}



export default Parent