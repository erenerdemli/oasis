import React, { Component } from 'react'
import styled from 'styled-components';

const Wrapper = styled.div`
  display: flex;
  padding: 10px;
  margin-right: 10px;
  border-radius: 10px;
  border: 1px solid #ffffff00;

  &:hover {
    border: 1px solid #ccc;
  }
`

const Image = styled.div`
  padding-right: 10px;
  opacity: ${props => props.acquired ? 1 : 0.1};
`

const Details = styled.div`
  flex: 1;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
  width: 180px;
`

const Title = styled.div`
  line-height: 2;
  font-weight: 700;
`

const Description = styled.div`
  font-size: 12px;
  flex: 1;
`

const MetaInfo = styled.div`
  justify-self: flex-end;
  line-height: 2;
  font-size: 12px;
  font-style: italic;
`

export default class Badge extends Component {
  render() {
    const { image, title, description, achievedDate, imageOnly = true, acquired = true } = this.props;

    return (
      <Wrapper>
        <Image acquired={acquired} {...this.props}>
          <img src={image} width={80} height={90} title={title} />
        </Image>
        {
          !imageOnly && <Details>
            <Title>{title}</Title>
            <Description>{description}</Description>
            {
              achievedDate && <MetaInfo>You achieved this on 9th Feb.</MetaInfo>
            }
          </Details>
        }
      </Wrapper>
    )
  }
}