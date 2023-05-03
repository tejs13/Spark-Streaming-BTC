import React, {Component} from 'react';
import { Breadcrumb, Layout, Menu} from 'antd';
import {  CodeOutlined, NodeExpandOutlined } from '@ant-design/icons';
import Parent from '../../LearningExcercise/ParentChild/Parent';
import SideBar from '../Components/SidebNavBar/SideBar';
import { menuItems } from '../Components/SidebNavBar/MenuItems';
import Clock from '../../LearningExcercise/ClockAndLifecycle/clock';
import DefaultScreen from '../DefaultScreen';
import BTCChart from '../../BigData/Screens/BTCLive';
import { Data } from '../../BigData/DataSource/sample';
import BTCBarChart from '../../BigData/Screens/BTCBarChart';

const { Header, Content, Footer, Sider } = Layout;



class Dashboard extends React.Component{
    constructor(props){
        super(props)
        this.state = {
            showSideNavBar : true,  
            currentActiveMenuKey : 0,
            currentBreadcrumb : null
            
        }
        this.toggleCollapse = this.toggleCollapse.bind(this)
       
    }

    toggleCollapse(e) {
        console.log(e)

        this.setState(this.state.showSideNavBar ? {showSideNavBar: false } : {showSideNavBar: true} )
    }

    handleNavClick = (item) => {
        console.log(item.key, this)
        let menuItem = menuItems.find(x => x.key === item.key)
        this.setState({currentBreadcrumb: menuItem.text,
                    currentActiveMenuKey: menuItem.key}, console.log(this.state))

    }



    render(){



        return(
            <>
            <Layout style={{ minHeight: '100vh' }}>

            
                {/* Side Navigation Bar */}
                <SideBar
                showSideNavBar={this.state.showSideNavBar}
                toggleCollapse={this.toggleCollapse}
                handleNavClick={this.handleNavClick}
                />



                <Layout className="site-layout">


                    <Header style={{ padding: 0, background: '#FCFEFF' }}  >
                        <h2 style={{ textAlign: 'center' }}>Dashboard Screen</h2>
                    </Header> 



                    <Content style={{ margin: '0 16px' }}>

                    <Breadcrumb style={{ margin: '16px 0' }}>
                        <Breadcrumb.Item>Exercise</Breadcrumb.Item>
                        <Breadcrumb.Item>{this.state.currentBreadcrumb}</Breadcrumb.Item>
                    </Breadcrumb>


                    {/* main content screen space  */}
                    <div style={{ padding: 24, minHeight: '70vh', background: '#FCFEFF' }}>

                    {
                            {
                            '0' : <DefaultScreen/>,
                            '1': <Parent/>,
                            '2': <Clock/>,
                            // '3' : <BTCChart data={Data}/>
                            '3' : <BTCBarChart/>
                            }[this.state.currentActiveMenuKey]
                     }
                        
                    </div>


                    </Content>



                    <Footer style={{ textAlign: 'center',padding: 14, minHeight: '18vh' }}>Blkcat.in ©2023 Created by Tejaskumar</Footer>
                
                
                </Layout>

            </Layout>
             

            </>
           

        )
    }
    
}

export default Dashboard