a = 10

console.log(a)

class car {
    constructor(brand){
        this.brand = brand
    }

    print_brand(){
        console.log(this.brand)
    }
}


class model extends car {
    constructor(brand, model){
        super(brand);
        this.model = model
    }

    printCarDetails(){
        console.log(this.brand +  '--' + this.model)   //direct access of super variables by this keywrd, as they are now part of child class
        super.print_brand() 
        console.log(this)                           //methods access with super keyword
    }
}

myCar = new model("Maruti Suzuki", 'Ritz')
myCar.printCarDetails()
console.log(this)


function printDate() {
  console.log(new Date().toLocaleDateString())
}

setInterval(printDate, 1000);

