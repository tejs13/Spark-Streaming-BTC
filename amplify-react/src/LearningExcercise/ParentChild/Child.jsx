import React, {Component} from 'react';


class Child extends React.Component {


    state = {}



    returnClickCount = (c) => {
        this.props.getClick(c)

    }
    render() {

        console.log(JSON.stringify(this.props) + 'asd')

        {this.returnClickCount(this.props.clickCount)}
        return (

            <>
            <p> Child Component click no. -- {this.props.clickCount}</p>
            <p> Down to 0 after 5 Clicks </p>
            </>
        )
    }

}

export default Child