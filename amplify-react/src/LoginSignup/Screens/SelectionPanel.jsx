import React, { Component } from 'react';
import { Card, Col, Row , Button} from 'antd';
import { withRouter } from '../../Commons/WithRouterWrapper';

class SelectionPanel extends React.Component {

    state = {}

    handleSelectionButtonClick = (e) => {
        console.log(e.target.id)
        this.props.navigate('/Dashboard')

    }


    render() {
        return (

            <>
                <div style={{backgroundColor: '#EAF0F6'}}>
                    <div>
                        <br />
                        <br />
                        <br />
                        <br />
                        <br />
                        <h1> Select The View :</h1>
                        <br />
                        <br />
                        <br />
                      

                    </div>

                    {/* <div style={{ background: '#CFD2D5'}}> */}

                    <Row gutter={16} justify="space-around" align="middle">
                        <Col span={9}>
                            <Card title="Explore" bordered={true} style={{ height: '100%' }}>
                                Card content
                                Card content
                                Card content
                                <br />
                                Card content
                                Card content
                                Card content

                                Card content
                                <br/>
                                <br/>
                                <br/>
                                <Button id='explore' onClick={this.handleSelectionButtonClick} type="primary" size='large'>Let's Begin</Button>
                            </Card>
                        </Col>

                        <Col span={9}>
                            <Card title="My Posts" bordered={true} style={{ height: '100%' }}>
                            Card content
                                Card content
                                Card content
                                <br />
                                Card content
                                Card content
                                Card content

                                Card content
                                <br/>
                                <br/>
                                <br/>
                                <Button id='mypost' onClick={this.handleSelectionButtonClick} type="primary" size='large'>Let's Begin</Button>
                            </Card>
                        </Col>
                    </Row>


                    <div>
                        <br />
                        <br />
                        <br />
                        <br />
                        <br />
                        <br />
                        <br />
                        <br />
                        <br />
                        
                      

                    </div>
                </div>

            </>
        )
    }

}

export default withRouter(SelectionPanel)