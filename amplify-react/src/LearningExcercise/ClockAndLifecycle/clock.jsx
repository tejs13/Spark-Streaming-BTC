import React, {Component} from 'react';


class Clock extends React.Component {
    state = {
        'date' : new Date().toLocaleDateString(),
    }


    componentDidMount(){
        console.log("IN MOUNT")
         this.timerID = setInterval(()=>this.tick(), 1000)

    }

    componentWillUnmount() {
        clearInterval(this.timerID);

    }
    
    tick () {
        this.setState({date: new Date().toLocaleDateString()})
    }


    render(){

        const today = new Date();
                let h = today.getHours();
                let m = today.getMinutes();
                let s = today.getSeconds();
                // m = checkTime(m);
                // s = checkTime(s);
                {console.log("in mmmm")}
        return (
            <>
            <h1>Clock Here</h1>
            
            <h1>{this.state.date}</h1>
            {
                <h1>{h} : {m} : {s}</h1>
            }
            </>
        )
    }
}


export default Clock