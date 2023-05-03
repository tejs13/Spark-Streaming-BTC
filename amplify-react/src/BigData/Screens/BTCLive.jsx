

import React, { Component } from 'react';

import { Bar, Line } from "react-chartjs-2";

import { Chart, registerables } from 'chart.js';
import StreamingPlugin from "chartjs-plugin-streaming";
import "chartjs-adapter-luxon";



Chart.register(...registerables, StreamingPlugin);




class BTCChart extends React.Component {
    state = {
        'date': new Date().toLocaleDateString(),
    }


    componentDidMount() {

    }

    componentWillUnmount() {

    }




    render() {


        return (
            <>
                <div className="chart-container">
                    <h1 style={{ textAlign: "center" }}>Real Time Analysis For BTC Transactions</h1>
                    <Line
                        data={{
                            datasets: [
                                {
                                    label: "Dataset 1",
                                    backgroundColor: "rgba(255, 99, 132, 0.5)",
                                    borderColor: "rgb(255, 99, 132)",
                                    borderDash: [8, 4],
                                    fill: true,
                                    data: []
                                },
                                {
                                    label: "Dataset 2",
                                    backgroundColor: "rgba(54, 162, 235, 0.5)",
                                    borderColor: "rgb(54, 162, 235)",
                                    cubicInterpolationMode: "monotone",
                                    fill: true,
                                    data: []
                                }
                            ]
                        }}
                        options={{
                            scales: {
                                x: {
                                    type: "realtime",
                                    realtime: {
                                        delay: 2000,
                                        onRefresh: (chart) => {
                                            chart.data.datasets.forEach((dataset) => {
                                                dataset.data.push({
                                                    x: Date.now(),
                                                    y: Math.random()
                                                });
                                            });
                                        }
                                    }
                                }
                            }
                        }}
                    />

                </div>











                {/* Simple BAr Chart */}
                {/* <div className="chart-container">
                    <h2 style={{ textAlign: "center" }}>Real Time Analysis For BTC Transactions</h2>
                    <Bar
                        data={this.props.data}
                        options={{
                            title: {
                                display: true,
                                text: 'BTC Transactions Analysis',
                                fontSize: 20
                            },
                            legend: {
                                display: true,
                                position: 'right'
                            }
                        }}
                    />
                </div> */}
            </>
        )
    }
}


export default BTCChart