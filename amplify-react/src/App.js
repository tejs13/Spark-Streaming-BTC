import logo from './logo.svg';
import './App.css';
import { BrowserRouter } from 'react-router-dom';
import ReactGA from 'react-ga';
import RouteChangeTracker from './RouteChangeTracker'
import HomeScreen from './HomeScreen/Screens/HomeScreen';
import AppRouters from './AppRouter';


const TRACKING_ID = "UA-252607377-1"; // YOUR_OWN_TRACKING_ID
ReactGA.initialize(TRACKING_ID);

function App() {
  return (
    <div className="App">
       <BrowserRouter>
            <AppRouters/>
       </BrowserRouter>
      
      {/* <RouteChangeTracker/> */}
      </div>
    
  );
}

export default App;
