import React, {Component} from 'react';

import { menuItems } from './MenuItems';
import { Layout, Menu} from 'antd';
import {  CodeOutlined, NodeExpandOutlined } from '@ant-design/icons';

const { Sider } = Layout;

class SideBar extends React.Component {
    state = {
    }



    render() {

        
        // Arrow funtion with curly braces doesnt mean return, explicitly return NodeExpandOutlined
        // if want without return , wrap with () and no } {}
        const sideMenuItems = menuItems.map((item) => {
            {console.log(item.key)}
             return <Menu.Item key={item.key}>
                                <CodeOutlined />
                                    <span>{item.text}</span>
                    </Menu.Item>
                    
        })
        return (
            <>
            {/* Side Navigation Bar */}
            <Sider collapsible collapsed={this.props.showSideNavBar} onCollapse={this.props.toggleCollapse}>
                <div style={{ height: 32, margin: 16, background: 'rgba(255, 255, 255, 0.2)' }} >
                <h1 style={{float: 'inherit'}}>Excs</h1>
                </div>

                {/* Side Navigation Bar Menu*/}
                <Menu theme="dark" defaultSelectedKeys={['1']} mode="inline" onClick={this.props.handleNavClick}>
                {sideMenuItems}
                </Menu>

            </Sider>
            </>

        )
    }

}

export default SideBar