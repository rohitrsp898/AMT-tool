function showDiv(element)
{
    document.getElementById('detail').style.display = element.value == 'detail' ? 'block' : 'none';
    
    document.getElementById('basic').style.display = element.value == 'basic' ? 'block' : 'none';
    
}

// $(document).delegate(".radio","change",function(){
//     //alert($(this).val());
//     var values=$(this).val();
//     if(values=="basic") {
//         $("#basic").attr("disabled", true);  
//     }
//     else {
//     $("#detail").attr("disabled", false);  
//     }
// });




// function yourfunction(radioid)
// {
// if(radioid == 'detail')
// {    
//    	document.getElementById('src1').style.display = '';    //display details dropdown
//     document.getElementById('src2').style.display = 'none';
// }
//  else if(radioid == 'basic')
// {  
// 	document.getElementById('src1').style.display = 'none';
// 	document.getElementById('src2').style.display = '';
  
// }
// }




// var radio_buttons = document.getElementsByName("radio");
// var dropdowns = document.getElementsByTagName("select");

// for (var i = 0; i < radio_buttons.length; i++) {
//     radio_buttons[i].addEventListener("change", setDropDown);
// }

// function setDropDown() {
//     setDropDownsForNoDisplay();
//     if (this.checked) {
//         setDropDownForDisplay(this.value);        
//     }
// }

// function setDropDownsForNoDisplay() {
//     for (var i = 0; i < dropdowns.length; i++) {
//         dropdowns[i].classList.add("src");
//     }
// }

// function setDropDownForDisplay(x) {
//     if (x === "detail") {
//         document.getElementById("detail").classList.remove("src");
//     } else if (x === "basic") {
//         document.getElementById("basic").classList.remove("src");
//     }
// }