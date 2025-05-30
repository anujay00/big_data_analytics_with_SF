import React, { useState } from "react";
import "./UserReviewPage.css";
import axios from "axios";

const UserReviewPage = () => {
  const [formData, setFormData] = useState({
    age: "",
    job: "",
    sector: "",
    monthlyIncome: "",
    gender: "",
    civilState: "",
    familyMembers: "",
    vehicleType: "",
    vehicleBrand: "",
    fuelType: ""
  });
  const [image, setImage] = useState(null);
  const [imagePreview, setImagePreview] = useState(null);
  const [errorMessage, setErrorMessage] = useState("");

  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData({
      ...formData,
      [name]: value
    });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const submitData = new FormData();
    
    // Add all form fields to FormData
    Object.keys(formData).forEach(key => {
      submitData.append(key, formData[key]);
    });
    
    // Add image if exists
    if (image) {
      submitData.append("image", image);
    }

    try {
      const response = await axios.post(
        "http://localhost:4000/api/reviews",
        submitData,
        {
          headers: {
            "Content-Type": "multipart/form-data",
          },
        }
      );
      console.log("Response:", response.data);
      alert("Review submitted successfully!");
      // Reset form
      setFormData({
        age: "",
        job: "",
        sector: "",
        monthlyIncome: "",
        gender: "",
        civilState: "",
        familyMembers: "",
        vehicleType: "",
        vehicleBrand: "",
        fuelType: ""
      });
      setImage(null);
      setImagePreview(null);
      setErrorMessage("");
    } catch (error) {
      console.error("Error submitting review:", error);
      setErrorMessage("Failed to submit the review. Please try again.");
    }
  };

  const handleImageChange = (e) => {
    const file = e.target.files[0];
    if (file) {
      setImage(file);
      setImagePreview(URL.createObjectURL(file));
    }
  };

  return (
    <div className="review-container">
      <h2>Submit Your Answer</h2>
      {errorMessage && <p className="error-message">{errorMessage}</p>}
      <form onSubmit={handleSubmit}>
        {/* Age */}
        <input
          type="number"
          name="age"
          placeholder="Age"
          value={formData.age}
          onChange={handleChange}
          required
        />
        <br /><br />

        {/* Job */}
        <select
          className="ratinginputu"
          name="job"
          value={formData.job}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Select your job.
          </option>
          <option value="Teacher">Teacher</option>
          <option value="Lawyer">Lawyer</option>
          <option value="Doctor">Doctor</option>
          <option value="Driver">Driver</option>
          <option value="Engineer">Engineer</option>
          <option value="Nurse">Nurse</option>
          <option value="Military">Military</option>
          <option value="Police">Police</option>
          <option value="Farmer">Farmer</option>
          <option value="Businessman">Businessman</option>
          <option value="Student">Student</option>
          <option value="Other">Other</option>
        </select>
        <br /><br />

        {/* Sector */}
        <select
          className="ratinginputu"
          name="sector"
          value={formData.sector}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Work in Government or Private Sector.
          </option>
          <option value="Government">Government</option>
          <option value="Private">Private</option>
          <option value="Other">Other</option>
        </select>
        <br /><br />

        {/* Monthly Income */}
        <input
          type="number"
          name="monthlyIncome"
          placeholder="Monthly Income"
          value={formData.monthlyIncome}
          onChange={handleChange}
          required
        />
        <br /><br />

        {/* Gender */}
        <select
          className="ratinginputu"
          name="gender"
          value={formData.gender}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Select Gender
          </option>
          <option value="Male">Male</option>
          <option value="Female">Female</option>
        </select>
        <br /><br />

        {/* Civil state */}
        <select
          className="ratinginputu"
          name="civilState"
          value={formData.civilState}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Select civil state
          </option>
          <option value="Married">Married</option>
          <option value="Unmarried">Unmarried</option>
        </select>
        <br /><br />

        {/* Family Members */}
        <input
          type="number"
          name="familyMembers"
          placeholder="Number of Family Members"
          value={formData.familyMembers}
          onChange={handleChange}
          required
        />
        <br /><br />

        {/* Vehicle type */}
        <select
          className="ratinginputu"
          name="vehicleType"
          value={formData.vehicleType}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Select vehicle type you drive or like.
          </option>
          <option value="Car">Car</option>
          <option value="Van">Van</option>
          <option value="Bike">Bike</option>
          <option value="Threewheel">Threewheel</option>
          <option value="SUV">SUV</option>
          <option value="Lorry">Lorry</option>
          <option value="Cab">Cab</option>
          <option value="Bus">Bus</option>
          <option value="Other">Other</option>
        </select>
        <br /><br />

        {/* Vehicle brand */}
        <select
          className="ratinginputu"
          name="vehicleBrand"
          value={formData.vehicleBrand}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Select Vehicle brand you drive or like.
          </option>
          <option value="Toyota">Toyota</option>
          <option value="Mitsubishi">Mitsubishi</option>
          <option value="Bajaj">Bajaj</option>
          <option value="Yamaha">Yamaha</option>
          <option value="BMW">BMW</option>
          <option value="Audi">Audi</option>
          <option value="Suzuki">Suzuki</option>
          <option value="Kia">Kia</option>
          <option value="Hyundai">Hyundai</option>
          <option value="Micro">Micro</option>
          <option value="perodua">perodua</option>
          <option value="Nissan">Nissan</option>
          <option value="Honda">Honda</option>
          <option value="Tesla">Tesla</option>
          <option value="MG">MG</option>
          <option value="Piagio">Piagio</option>
          <option value="Dihatsu">Dihatsu</option>
          <option value="Tata">Tata</option>
          <option value="Ford">Ford</option>
          <option value="Chevrolet">Chevrolet</option>
          <option value="Volvo">Volvo</option>
          <option value="Benz">Benz</option>
          <option value="Other">Other</option>
        </select>
        <br /><br />

        {/* Vehicle Fuel */}
        <select
          className="ratinginputu"
          name="fuelType"
          value={formData.fuelType}
          onChange={handleChange}
          required
        >
          <option value="" disabled>
            Fuel type.
          </option>
          <option value="Petrol">Petrol</option>
          <option value="Diesel">Diesel</option> {/* Fixed typo: "Disel" to "Diesel" */}
          <option value="EV">EV</option>
        </select>
        <br /><br />

        {/* Image upload */}
        <div className="image-upload">
          <label htmlFor="image">Upload Image (Optional):</label>
          <input
            type="file"
            id="image"
            name="image"
            accept="image/*"
            onChange={handleImageChange}
          />
          {imagePreview && (
            <div className="image-preview">
              <img src={imagePreview} alt="Preview" />
            </div>
          )}
        </div>
        <br /><br />

        <button type="submit">Submit</button>
      </form>
    </div>
  );
};

export default UserReviewPage;